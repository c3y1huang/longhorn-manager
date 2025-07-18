package controller

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/scheduler"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	"github.com/longhorn/longhorn-manager/controller/monitor"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	nodeControllerResyncPeriod = 30 * time.Second
	ignoreKubeletNotReadyTime  = 15 * time.Second

	unknownDiskID = "UNKNOWN_DISKID"

	snapshotChangeEventQueueMax = 1048576
)

type NodeController struct {
	*baseController

	// which namespace controller is running with
	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	diskMonitor             monitor.Monitor
	environmentCheckMonitor monitor.Monitor

	snapshotMonitor              monitor.Monitor
	snapshotChangeEventQueue     workqueue.TypedInterface[any]
	snapshotChangeEventQueueLock sync.Mutex

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	topologyLabelsChecker TopologyLabelsChecker

	scheduler *scheduler.ReplicaScheduler
}

type TopologyLabelsChecker func(kubeClient clientset.Interface, vers string) (bool, error)

func NewNodeController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace, controllerID string) (*NodeController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	nc := &NodeController{
		baseController: newBaseController("longhorn-node", logger),

		namespace:    namespace,
		controllerID: controllerID,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-node-controller"}),

		ds: ds,

		topologyLabelsChecker: util.IsKubernetesVersionAtLeast,

		snapshotChangeEventQueue: workqueue.NewTyped[any](),
	}

	nc.scheduler = scheduler.NewReplicaScheduler(ds)

	var err error
	// We want to check the real time usage of disk on nodes.
	// Therefore, we add a small resync for the NodeInformer here
	if _, err = ds.NodeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    nc.enqueueNode,
		UpdateFunc: func(old, cur interface{}) { nc.enqueueNode(cur) },
		DeleteFunc: nc.enqueueNode,
	}, nodeControllerResyncPeriod); err != nil {
		return nil, err
	}

	nc.cacheSyncs = append(nc.cacheSyncs, ds.NodeInformer.HasSynced)

	if _, err = ds.SettingInformer.AddEventHandlerWithResyncPeriod(
		cache.FilteringResourceEventHandler{
			FilterFunc: nc.isResponsibleForSetting,
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    nc.enqueueSetting,
				UpdateFunc: func(old, cur interface{}) { nc.enqueueSetting(cur) },
			},
		}, 0); err != nil {
		return nil, err
	}
	nc.cacheSyncs = append(nc.cacheSyncs, ds.SettingInformer.HasSynced)

	if _, err = ds.ReplicaInformer.AddEventHandlerWithResyncPeriod(
		cache.FilteringResourceEventHandler{
			FilterFunc: nc.isResponsibleForReplica,
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    nc.enqueueReplica,
				UpdateFunc: func(old, cur interface{}) { nc.enqueueReplica(cur) },
				DeleteFunc: nc.enqueueReplica,
			},
		}, 0); err != nil {
		return nil, err
	}
	nc.cacheSyncs = append(nc.cacheSyncs, ds.ReplicaInformer.HasSynced)

	if _, err = ds.SnapshotInformer.AddEventHandlerWithResyncPeriod(
		cache.FilteringResourceEventHandler{
			FilterFunc: nc.isResponsibleForSnapshot,
			Handler: cache.ResourceEventHandlerFuncs{
				UpdateFunc: func(old, cur interface{}) { nc.enqueueSnapshot(old, cur) },
			},
		}, 0); err != nil {
		return nil, err
	}
	nc.cacheSyncs = append(nc.cacheSyncs, ds.SnapshotInformer.HasSynced)

	if _, err = ds.PodInformer.AddEventHandlerWithResyncPeriod(
		cache.FilteringResourceEventHandler{
			FilterFunc: isManagerPod,
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    nc.enqueueManagerPod,
				UpdateFunc: func(old, cur interface{}) { nc.enqueueManagerPod(cur) },
				DeleteFunc: nc.enqueueManagerPod,
			},
		}, 0); err != nil {
		return nil, err
	}
	nc.cacheSyncs = append(nc.cacheSyncs, ds.PodInformer.HasSynced)

	if _, err = ds.KubeNodeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(old, cur interface{}) { nc.enqueueKubernetesNode(cur) },
		DeleteFunc: nc.enqueueKubernetesNode,
	}, 0); err != nil {
		return nil, err
	}
	nc.cacheSyncs = append(nc.cacheSyncs, ds.KubeNodeInformer.HasSynced)

	return nc, nil
}

func (nc *NodeController) isResponsibleForSetting(obj interface{}) bool {
	setting, ok := obj.(*longhorn.Setting)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return false
		}

		// use the last known state, to enqueue, dependent objects
		setting, ok = deletedState.Obj.(*longhorn.Setting)
		if !ok {
			return false
		}
	}

	return types.SettingName(setting.Name) == types.SettingNameStorageMinimalAvailablePercentage ||
		types.SettingName(setting.Name) == types.SettingNameBackingImageCleanupWaitInterval ||
		types.SettingName(setting.Name) == types.SettingNameOrphanResourceAutoDeletion ||
		types.SettingName(setting.Name) == types.SettingNameNodeDrainPolicy
}

func (nc *NodeController) isResponsibleForReplica(obj interface{}) bool {
	replica, ok := obj.(*longhorn.Replica)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return false
		}

		// use the last known state, to enqueue, dependent objects
		replica, ok = deletedState.Obj.(*longhorn.Replica)
		if !ok {
			return false
		}
	}

	return replica.Spec.NodeID == nc.controllerID
}

func (nc *NodeController) isResponsibleForSnapshot(obj interface{}) bool {
	snapshot, ok := obj.(*longhorn.Snapshot)
	if !ok {
		return false
	}
	volumeName, ok := snapshot.Labels[types.LonghornLabelVolume]
	if !ok {
		nc.logger.Warnf("Failed to find volume name from snapshot %v", snapshot.Name)
		return false
	}
	volume, err := nc.ds.GetVolumeRO(volumeName)
	if err != nil {
		nc.logger.WithError(err).Warnf("Failed to get volume for snapshot %v", snapshot.Name)
		return false
	}
	if volume.Status.OwnerID != nc.controllerID {
		return false
	}

	return nc.snapshotHashRequired(volume)
}

func (nc *NodeController) snapshotHashRequired(volume *longhorn.Volume) bool {
	dataIntegrityImmediateChecking, err := nc.ds.GetSettingAsBool(types.SettingNameSnapshotDataIntegrityImmediateCheckAfterSnapshotCreation)
	if err != nil {
		nc.logger.WithError(err).Warnf("Failed to get %v setting", types.SettingNameSnapshotDataIntegrityImmediateCheckAfterSnapshotCreation)
		return false
	}
	if !dataIntegrityImmediateChecking {
		return false
	}

	dataIntegrity, err := nc.ds.GetVolumeSnapshotDataIntegrity(volume.Name)
	if err != nil {
		return false
	}

	return dataIntegrity != longhorn.SnapshotDataIntegrityDisabled
}

func isManagerPod(obj interface{}) bool {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return false
		}

		// use the last known state, to enqueue, dependent objects
		pod, ok = deletedState.Obj.(*corev1.Pod)
		if !ok {
			return false
		}
	}

	for _, con := range pod.Spec.Containers {
		if con.Name == "longhorn-manager" {
			return true
		}
	}
	return false
}

func (nc *NodeController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer nc.queue.ShutDown()

	nc.logger.Info("Starting Longhorn node controller")
	defer nc.logger.Info("Shut down Longhorn node controller")

	if !cache.WaitForNamedCacheSync("longhorn node", stopCh, nc.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(nc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (nc *NodeController) worker() {
	for nc.processNextWorkItem() {
	}
}

func (nc *NodeController) processNextWorkItem() bool {
	key, quit := nc.queue.Get()

	if quit {
		return false
	}
	defer nc.queue.Done(key)

	err := nc.syncNode(key.(string))
	nc.handleErr(err, key)

	return true
}

func (nc *NodeController) handleErr(err error, key interface{}) {
	if err == nil {
		nc.queue.Forget(key)
		return
	}

	log := nc.logger.WithField("LonghornNode", key)
	if nc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn node")
		nc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn node out of the queue")
	nc.queue.Forget(key)
}

func getLoggerForNode(logger logrus.FieldLogger, n *longhorn.Node) *logrus.Entry {
	return logger.WithField("node", n.Name)
}

func (nc *NodeController) syncNode(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync node for %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != nc.namespace {
		// Not ours, don't do anything
		return nil
	}

	node, err := nc.ds.GetNode(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			nc.logger.Errorf("Longhorn node %v has been deleted", key)
			return nil
		}
		return err
	}

	log := getLoggerForNode(nc.logger, node)

	if node.DeletionTimestamp != nil {
		nc.eventRecorder.Eventf(node, corev1.EventTypeWarning, constant.EventReasonDelete, "Deleting node %v", node.Name)
		return nc.ds.RemoveFinalizerForNode(node)
	}

	existingNode := node.DeepCopy()
	defer func() {
		// we're going to update node assume things changes
		if err == nil && !reflect.DeepEqual(existingNode.Status, node.Status) {
			_, err = nc.ds.UpdateNodeStatus(node)
		}
		// requeue if it's conflict
		if apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue %v due to conflict", key)
			nc.enqueueNode(node)
			err = nil
		}
	}()

	managerPods, err := nc.ds.ListManagerPodsRO()
	if err != nil {
		return err
	}

	kubeNode, err := nc.ds.GetKubernetesNodeRO(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			// Directly record condition and return. The Kubernetes node controller should delete this Longhorn node
			// very soon. If we continue to reconcile with a nil pointer (e.g. on a node that is being removed), we are
			// guaranteed to run into an exception later on anyways.
			node.Status.Conditions = types.SetConditionAndRecord(node.Status.Conditions,
				longhorn.NodeConditionTypeReady, longhorn.ConditionStatusFalse,
				string(longhorn.NodeConditionReasonKubernetesNodeGone),
				fmt.Sprintf("Kubernetes node missing: node %v has been removed from the cluster", node.Name),
				nc.eventRecorder, node, corev1.EventTypeWarning)
			return nil
		}
		return err
	}

	if err = nc.setReadyAndSchedulableConditions(node, kubeNode, managerPods); err != nil {
		return err
	}

	// Set any RWX leases to non-delinquent if owned by not-ready node.
	// Usefulness of delinquent state has passed.
	if err = nc.clearDelinquentLeasesIfNodeNotReady(node); err != nil {
		log.WithError(err).Warnf("Failed to clear delinquent leases for node %v", node.Name)
		return err
	}

	node.Status.Region, node.Status.Zone = types.GetRegionAndZone(kubeNode.Labels)

	if nc.controllerID != node.Name {
		return nil
	}

	// Getting here is enough proof of life to turn on the services that might
	// have been turned off for RWX failover.
	labels := types.MergeStringMaps(types.GetAdmissionWebhookLabel(), types.GetRecoveryBackendLabel())
	if err := nc.ds.AddLabelToManagerPod(node.Name, labels); err != nil {
		log.WithError(err).Error("Failed to restore its admission webhook and recovery backend")
		return err
	}

	// Create a monitor for collecting disk information
	if _, err := nc.createDiskMonitor(); err != nil {
		return err
	}

	// Create a monitor for collecting environment check information
	if _, err := nc.createEnvironmentCheckMonitor(); err != nil {
		return err
	}

	collectedDiskInfo, err := nc.syncWithDiskMonitor(node)
	if err != nil {
		if strings.Contains(err.Error(), "mismatching disks") {
			log.WithError(err).Warn("Failed to sync with disk monitor due to mismatching disks")
			return nil
		}
		return err
	}

	// sync disks status on current node
	if err := nc.syncDiskStatus(node, collectedDiskInfo); err != nil {
		return err
	}

	collectedEnvironmentCheckConditions, err := nc.syncWithEnvironmentCheckMonitor()
	if err == nil {
		// Best effort to update the environment check conditions
		nc.syncEnvironmentCheckConditions(node, collectedEnvironmentCheckConditions)
	}

	_, err = nc.createSnapshotMonitor()
	if err != nil {
		return errors.Wrap(err, "failed to create a snapshot monitor")
	}

	if nc.snapshotMonitor != nil {
		data, _ := nc.snapshotMonitor.GetCollectedData()
		status, ok := data.(monitor.SnapshotMonitorStatus)
		if !ok {
			log.Errorf("Failed to assert value from snapshot monitor: %v", data)
		} else {
			node.Status.SnapshotCheckStatus.LastPeriodicCheckedAt = status.LastSnapshotPeriodicCheckedAt
		}
	}

	// sync mount propagation status on current node
	for _, pod := range managerPods {
		if pod.Spec.NodeName == node.Name {
			if err := nc.syncNodeStatus(pod, node); err != nil {
				return err
			}
		}
	}

	if err := nc.syncInstanceManagers(node); err != nil {
		return err
	}

	if err := nc.cleanUpBackingImagesInDisks(node); err != nil {
		return err
	}

	if err := nc.syncOrphans(node, collectedDiskInfo); err != nil {
		return err
	}

	if err = nc.syncReplicaEvictionRequested(node, kubeNode); err != nil {
		return err
	}

	if err := nc.syncBackingImageEvictionRequested(node); err != nil {
		return err
	}

	return nil
}

func (nc *NodeController) enqueueNode(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	nc.queue.Add(key)
}

func (nc *NodeController) enqueueNodeRateLimited(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	nc.queue.AddRateLimited(key)
}

func (nc *NodeController) enqueueSetting(obj interface{}) {
	nodes, err := nc.ds.ListNodesRO()
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to list nodes: %v ", err))
		return
	}

	for _, node := range nodes {
		nc.enqueueNode(node)
	}
}

func (nc *NodeController) enqueueReplica(obj interface{}) {
	replica, ok := obj.(*longhorn.Replica)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		replica, ok = deletedState.Obj.(*longhorn.Replica)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	node, err := nc.ds.GetNodeRO(replica.Spec.NodeID)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("failed to get node %v for replica %v: %v ",
				replica.Spec.NodeID, replica.Name, err))
		}
		return
	}
	nc.enqueueNode(node)
}

func (nc *NodeController) enqueueSnapshot(old, cur interface{}) {
	currentSnapshot, ok := cur.(*longhorn.Snapshot)
	if !ok {
		return
	}

	// Skip volume-head because it is not a real snapshot.
	// A system-generated snapshot is also ignored, because the prune operations the snapshots are out of
	// sync during replica rebuilding. More investigation is in https://github.com/longhorn/longhorn/issues/4513
	if !currentSnapshot.Status.UserCreated {
		return
	}

	volumeName, ok := currentSnapshot.Labels[types.LonghornLabelVolume]
	if !ok {
		nc.logger.Warnf("Failed to get volume name from snapshot %v", currentSnapshot.Name)
		return
	}

	volume, err := nc.ds.GetVolumeRO(volumeName)
	if err != nil {
		nc.logger.WithError(err).Warnf("Failed to get volume %v", currentSnapshot.Name)
		return
	}

	if volume.Status.OwnerID != nc.controllerID {
		return
	}

	nc.snapshotChangeEventQueueLock.Lock()
	defer nc.snapshotChangeEventQueueLock.Unlock()
	// To avoid the snapshot events run out of the system memory, just ignore
	// the events. The events will be processed in following periodic rounds.
	if nc.snapshotChangeEventQueue.Len() < snapshotChangeEventQueueMax {
		nc.snapshotChangeEventQueue.Add(monitor.SnapshotChangeEvent{
			VolumeName:   volume.Name,
			SnapshotName: currentSnapshot.Name,
		})
	} else {
		nc.logger.Warnf("Dropped the snapshot change event with volume %v snapshot %v since snapshotChangeEventQueue is full",
			volume.Name, currentSnapshot.Name)
	}
}

func (nc *NodeController) enqueueManagerPod(obj interface{}) {
	nodes, err := nc.ds.ListNodesRO()
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to list nodes: %v ", err))
		return
	}
	for _, node := range nodes {
		nc.enqueueNode(node)
	}
}

func (nc *NodeController) enqueueKubernetesNode(obj interface{}) {
	kubernetesNode, ok := obj.(*corev1.Node)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		kubernetesNode, ok = deletedState.Obj.(*corev1.Node)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	nodeRO, err := nc.ds.GetNodeRO(kubernetesNode.Name)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("failed to get longhorn node %v: %v ", kubernetesNode.Name, err))
		}
		return
	}
	nc.enqueueNode(nodeRO)
}

func (nc *NodeController) syncDiskStatus(node *longhorn.Node, collectedDataInfo map[string]*monitor.CollectedDiskInfo) error {
	nc.alignDiskSpecAndStatus(node)

	notReadyDiskInfoMap, readyDiskInfoMap := nc.findNotReadyAndReadyDiskMaps(node, collectedDataInfo)

	for _, diskInfoMap := range notReadyDiskInfoMap {
		nc.updateNotReadyDiskStatusReadyCondition(node, diskInfoMap)
	}

	for _, diskInfoMap := range readyDiskInfoMap {
		nc.updateReadyDiskStatusReadyCondition(node, diskInfoMap)
		nc.updateDiskStatusFileSystemType(node, diskInfoMap)
	}

	return nc.updateDiskStatusSchedulableCondition(node)
}

func (nc *NodeController) syncEnvironmentCheckConditions(node *longhorn.Node, conditions []longhorn.Condition) {
	// Add condition to node.status.conditions if it is not already there
	// Update the condition status and reason if it is already in the node.status.conditions
	for _, condition := range conditions {
		found := false
		for _, existingCondition := range node.Status.Conditions {
			if existingCondition.Type == condition.Type {
				node.Status.Conditions = types.SetCondition(node.Status.Conditions, condition.Type, condition.Status, condition.Reason, condition.Message)
				found = true
				break
			}
		}
		if !found {
			// Add condition to node.status.conditions if it is not already there
			node.Status.Conditions = append(node.Status.Conditions, condition)
		}
	}

	isV2DataEngine, err := nc.ds.GetSettingAsBool(types.SettingNameV2DataEngine)
	if err != nil {
		nc.logger.WithError(err).Debug("Failed to fetch v2-data-engine setting")
		isV2DataEngine = false
	}

	if !isV2DataEngine {
		node.Status.Conditions = types.RemoveCondition(node.Status.Conditions, longhorn.NodeConditionTypeHugePagesAvailable)
	}
}

func (nc *NodeController) findNotReadyAndReadyDiskMaps(node *longhorn.Node, collectedDataInfo map[string]*monitor.CollectedDiskInfo) (notReadyDiskInfoMap, readyDiskInfoMap map[string]map[string]*monitor.CollectedDiskInfo) {
	notReadyDiskInfoMap = make(map[string]map[string]*monitor.CollectedDiskInfo, 0)
	readyDiskInfoMap = make(map[string]map[string]*monitor.CollectedDiskInfo, 0)
	diskIDDiskGroupMap := make(map[string]map[string]*monitor.CollectedDiskInfo, 0)

	// Find out notReadyDiskInfoMap and diskIDDiskGroupMap
	// diskID definition
	// a filesystem-type disk: fsid
	// a block-type disk: <major-number>-<minor-number> of the device
	for diskName, diskInfo := range collectedDataInfo {
		diskID := unknownDiskID
		if diskInfo.DiskStat != nil {
			diskID = diskInfo.DiskStat.DiskID
		}

		if notReadyDiskInfoMap[diskID] == nil {
			notReadyDiskInfoMap[diskID] = make(map[string]*monitor.CollectedDiskInfo, 0)
		}
		if diskIDDiskGroupMap[diskID] == nil {
			diskIDDiskGroupMap[diskID] = make(map[string]*monitor.CollectedDiskInfo, 0)
		}

		// The disk info collected by the node monitor will contain the condition only when the disk is NotReady
		if diskInfo.Condition != nil {
			notReadyDiskInfoMap[diskID][diskName] = diskInfo
			continue
		}

		// Move the disks with UUID config file missing error and UUID mismatching error to notReadyDiskInfoMap
		if node.Status.DiskStatus[diskName].DiskUUID != "" {
			errorMessage := ""
			if diskInfo.DiskUUID == "" {
				errorMessage = fmt.Sprintf("Disk %v(%v) on node %v is not ready: cannot find disk config file, maybe due to a mount error",
					diskName, diskInfo.Path, node.Name)
			} else if node.Status.DiskStatus[diskName].DiskUUID != diskInfo.DiskUUID {
				errorMessage = fmt.Sprintf("Disk %v(%v) on node %v is not ready: record diskUUID doesn't match the one on the disk ",
					diskName, diskInfo.Path, node.Name)
			}

			if errorMessage != "" {
				notReadyDiskInfoMap[diskID][diskName] =
					monitor.NewDiskInfo(diskInfo.DiskName, diskInfo.DiskUUID, diskInfo.Path, diskInfo.DiskDriver,
						diskInfo.NodeOrDiskEvicted, diskInfo.DiskStat,
						diskInfo.OrphanedReplicaDataStores,
						diskInfo.InstanceManagerName,
						string(longhorn.DiskConditionReasonDiskFilesystemChanged), errorMessage)
				continue
			}
		}

		diskIDDiskGroupMap[diskID][diskName] = diskInfo
	}

	// Find the duplicated disks and move them to notReadyDiskInfoMap
	for diskID, diskInfoMap := range diskIDDiskGroupMap {
		for diskName, diskInfo := range diskInfoMap {
			if readyDiskInfoMap[diskID] == nil {
				readyDiskInfoMap[diskID] = make(map[string]*monitor.CollectedDiskInfo, 0)
			}

			if nc.isDiskIDDuplicatedWithExistingReadyDisk(diskName, diskInfoMap, node.Status.DiskStatus) ||
				isReadyDiskFound(readyDiskInfoMap[diskID]) {
				notReadyDiskInfoMap[diskID][diskName] =
					monitor.NewDiskInfo(diskInfo.DiskName, diskInfo.DiskUUID, diskInfo.Path, diskInfo.DiskDriver, diskInfo.NodeOrDiskEvicted, diskInfo.DiskStat,
						diskInfo.OrphanedReplicaDataStores,
						diskInfo.InstanceManagerName,
						string(longhorn.DiskConditionReasonDiskFilesystemChanged),
						fmt.Sprintf("Disk %v(%v) on node %v is not ready: disk has same file system ID %v as other disks %+v",
							diskName, diskInfoMap[diskName].Path, node.Name, diskID, monitor.GetDiskNamesFromDiskMap(diskInfoMap)))
				continue
			}

			node.Status.DiskStatus[diskName].DiskUUID = diskInfo.DiskUUID
			node.Status.DiskStatus[diskName].DiskDriver = diskInfo.DiskDriver
			node.Status.DiskStatus[diskName].DiskName = diskInfo.DiskName
			node.Status.DiskStatus[diskName].DiskPath = diskInfo.Path

			readyDiskInfoMap[diskID][diskName] = diskInfo
		}
	}

	return notReadyDiskInfoMap, readyDiskInfoMap
}

func (nc *NodeController) updateNotReadyDiskStatusReadyCondition(node *longhorn.Node, diskInfoMap map[string]*monitor.CollectedDiskInfo) {
	for diskName, info := range diskInfoMap {
		if info.Condition == nil {
			continue
		}
		node.Status.DiskStatus[diskName].Conditions =
			types.SetConditionAndRecord(node.Status.DiskStatus[diskName].Conditions,
				longhorn.DiskConditionTypeReady,
				info.Condition.Status,
				info.Condition.Reason,
				info.Condition.Message,
				nc.eventRecorder,
				node,
				corev1.EventTypeWarning)
	}
}

func (nc *NodeController) updateReadyDiskStatusReadyCondition(node *longhorn.Node, diskInfoMap map[string]*monitor.CollectedDiskInfo) {
	diskStatusMap := node.Status.DiskStatus

	for diskName, info := range diskInfoMap {
		diskStatus := diskStatusMap[diskName]

		if diskStatus.DiskUUID == info.DiskUUID {
			usableStorage := diskInfoMap[diskName].DiskStat.StorageAvailable
			if diskStatus.Type == longhorn.DiskTypeFilesystem {
				// on the default disks this will be updated constantly since there is always something generating new disk usage (logs, etc)
				// We also don't need byte/block precisions for this instead we can round down to the next 10/100mb
				const truncateTo = 100 * 1024 * 1024
				usableStorage = (diskInfoMap[diskName].DiskStat.StorageAvailable / truncateTo) * truncateTo
			}
			diskStatus.StorageAvailable = usableStorage
			diskStatus.StorageMaximum = diskInfoMap[diskName].DiskStat.StorageMaximum
			diskStatus.InstanceManagerName = diskInfoMap[diskName].InstanceManagerName
			diskStatusMap[diskName].Conditions = types.SetConditionAndRecord(diskStatusMap[diskName].Conditions,
				longhorn.DiskConditionTypeReady, longhorn.ConditionStatusTrue,
				"", fmt.Sprintf("Disk %v(%v) on node %v is ready", diskName, diskInfoMap[diskName].Path, node.Name),
				nc.eventRecorder, node, corev1.EventTypeNormal)
		}
		diskStatusMap[diskName] = diskStatus
	}
}

func (nc *NodeController) updateDiskStatusFileSystemType(node *longhorn.Node, diskInfoMap map[string]*monitor.CollectedDiskInfo) {
	diskStatusMap := node.Status.DiskStatus
	for diskName, info := range diskInfoMap {
		diskStatus := diskStatusMap[diskName]
		if diskStatus.DiskUUID == info.DiskUUID && diskStatus.Type == longhorn.DiskTypeFilesystem {
			diskStatus.FSType = info.DiskStat.Type
		}
		diskStatusMap[diskName] = diskStatus
	}
}

func (nc *NodeController) updateDiskStatusSchedulableCondition(node *longhorn.Node) error {
	log := getLoggerForNode(nc.logger, node)

	diskStatusMap := node.Status.DiskStatus

	// update Schedulable condition
	backingImages, err := nc.ds.ListBackingImagesRO()
	if err != nil {
		return err
	}

	for diskName, disk := range node.Spec.Disks {
		diskStatus := diskStatusMap[diskName]

		if types.GetCondition(diskStatus.Conditions, longhorn.DiskConditionTypeReady).Status != longhorn.ConditionStatusTrue {
			diskStatus.StorageScheduled = 0
			diskStatus.ScheduledReplica = map[string]int64{}
			diskStatus.Conditions = types.SetConditionAndRecord(diskStatus.Conditions,
				longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusFalse,
				string(longhorn.DiskConditionReasonDiskNotReady),
				fmt.Sprintf("Disk %v (%v) on the node %v is not ready", diskName, disk.Path, node.Name),
				nc.eventRecorder, node, corev1.EventTypeWarning)
		} else {
			// sync backing image managers
			list, err := nc.ds.ListBackingImageManagersByDiskUUID(diskStatus.DiskUUID)
			if err != nil {
				return err
			}
			for _, bim := range list {
				if bim.Spec.NodeID != node.Name || bim.Spec.DiskPath != disk.Path {
					log.Infof("Node Controller: updating Node & disk info in backing image manager %v", bim.Name)
					bim.Spec.NodeID = node.Name
					bim.Spec.DiskPath = disk.Path
					if _, err := nc.ds.UpdateBackingImageManager(bim); err != nil {
						log.WithError(err).Warnf("Failed to update node & disk info for backing image manager %v when syncing disk %v(%v), will enqueue then resync node", bim.Name, diskName, diskStatus.DiskUUID)
						nc.enqueueNode(node)
						continue
					}
				}
			}

			// sync replicas as well as calculate storage scheduled
			replicas, err := nc.ds.ListReplicasByDiskUUID(diskStatus.DiskUUID)
			if err != nil {
				return err
			}
			scheduledReplica := map[string]int64{}
			scheduledBackingImage := map[string]int64{}
			storageScheduled := int64(0)
			for _, replica := range replicas {
				if replica.Spec.NodeID != node.Name || replica.Spec.DiskPath != disk.Path {
					replica.Spec.NodeID = node.Name
					replica.Spec.DiskPath = disk.Path
					if _, err := nc.ds.UpdateReplica(replica); err != nil {
						log.Warnf("Failed to update node & disk info for replica %v when syncing disk %v(%v), will enqueue then resync node", replica.Name, diskName, diskStatus.DiskUUID)
						nc.enqueueNode(node)
						continue
					}
				}
				storageScheduled += replica.Spec.VolumeSize
				scheduledReplica[replica.Name] = replica.Spec.VolumeSize
			}

			for _, backingImage := range backingImages {
				if _, exists := backingImage.Spec.DiskFileSpecMap[diskStatus.DiskUUID]; exists {
					storageScheduled += backingImage.Status.RealSize
					scheduledBackingImage[backingImage.Name] = backingImage.Status.RealSize
				}
			}

			diskStatus.StorageScheduled = storageScheduled
			diskStatus.ScheduledReplica = scheduledReplica
			diskStatus.ScheduledBackingImage = scheduledBackingImage
			// check disk pressure
			info, err := nc.scheduler.GetDiskSchedulingInfo(disk, diskStatus)
			if err != nil {
				return err
			}

			isSchedulableToDisk, message := nc.scheduler.IsSchedulableToDisk(0, 0, info)
			if !isSchedulableToDisk {
				diskStatus.Conditions = types.SetConditionAndRecord(diskStatus.Conditions,
					longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusFalse,
					string(longhorn.DiskConditionReasonDiskPressure),
					fmt.Sprintf("Disk %v (%v) on the node %v is not schedulable for more replica; %s", diskName, disk.Path, node.Name, message),
					nc.eventRecorder, node, corev1.EventTypeWarning)
			} else {
				diskStatus.Conditions = types.SetConditionAndRecord(diskStatus.Conditions,
					longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue,
					"", fmt.Sprintf("Disk %v(%v) on node %v is schedulable", diskName, disk.Path, node.Name),
					nc.eventRecorder, node, corev1.EventTypeNormal)
			}
		}

		diskStatusMap[diskName] = diskStatus
	}

	return nil
}

func (nc *NodeController) syncNodeStatus(pod *corev1.Pod, node *longhorn.Node) error {
	// sync bidirectional mount propagation for node status to check whether the node could deploy CSI driver
	var mgrContainer *corev1.Container
	for _, container := range pod.Spec.Containers {
		if container.Name == types.LonghornManagerContainerName {
			mgrContainer = &container
			break
		}
	}
	if mgrContainer == nil {
		return fmt.Errorf("failed to find the %v container in the pod %v", types.LonghornManagerDaemonSetName, pod.Name)
	}
	for _, mount := range mgrContainer.VolumeMounts {
		if mount.Name == types.LonghornSystemKey {
			mountPropagationStr := ""
			if mount.MountPropagation != nil {
				mountPropagationStr = string(*mount.MountPropagation)
			}
			if mountPropagationStr != string(corev1.MountPropagationBidirectional) {
				node.Status.Conditions = types.SetCondition(node.Status.Conditions, longhorn.NodeConditionTypeMountPropagation, longhorn.ConditionStatusFalse,
					string(longhorn.NodeConditionReasonNoMountPropagationSupport),
					fmt.Sprintf("The MountPropagation value %s is not detected from pod %s, node %s", mountPropagationStr, pod.Name, pod.Spec.NodeName))
			} else {
				node.Status.Conditions = types.SetCondition(node.Status.Conditions, longhorn.NodeConditionTypeMountPropagation, longhorn.ConditionStatusTrue, "", "")
			}
			break
		}
	}

	return nil
}

func (nc *NodeController) getImTypeDataEngines(node *longhorn.Node) map[longhorn.InstanceManagerType][]longhorn.DataEngineType {
	log := getLoggerForNode(nc.logger, node)

	// TODO: remove InstanceManagerTypeEngine and InstanceManagerTypeReplica in the future.
	dataEngines := map[longhorn.InstanceManagerType][]longhorn.DataEngineType{
		longhorn.InstanceManagerTypeAllInOne: {},
		longhorn.InstanceManagerTypeEngine:   {},
		longhorn.InstanceManagerTypeReplica:  {},
	}

	for _, setting := range []types.SettingName{types.SettingNameV1DataEngine, types.SettingNameV2DataEngine} {
		enabled, err := nc.ds.GetSettingAsBool(setting)
		if err != nil {
			log.WithError(err).Warnf("Failed to get %v setting", setting)
			continue
		}
		if !enabled {
			continue
		}

		switch setting {
		case types.SettingNameV1DataEngine:
			dataEngines[longhorn.InstanceManagerTypeAllInOne] = append(dataEngines[longhorn.InstanceManagerTypeAllInOne], longhorn.DataEngineTypeV1)
			dataEngines[longhorn.InstanceManagerTypeEngine] = append(dataEngines[longhorn.InstanceManagerTypeEngine], longhorn.DataEngineTypeV1)
			if len(node.Spec.Disks) != 0 {
				dataEngines[longhorn.InstanceManagerTypeReplica] = append(dataEngines[longhorn.InstanceManagerTypeReplica], longhorn.DataEngineTypeV1)
			}
		case types.SettingNameV2DataEngine:
			if _, err := nc.ds.ValidateV2DataEngineEnabled(enabled); err == nil {
				dataEngines[longhorn.InstanceManagerTypeAllInOne] = append(dataEngines[longhorn.InstanceManagerTypeAllInOne], longhorn.DataEngineTypeV2)
			} else {
				log.WithError(err).Warnf("Failed to validate %v setting", types.SettingNameV2DataEngine)
			}
		}
	}

	return dataEngines
}

func (nc *NodeController) cleanupAllReplicaManagers(node *longhorn.Node) error {
	log := getLoggerForNode(nc.logger, node)

	rmMap, err := nc.ds.ListInstanceManagersByNodeRO(node.Name, longhorn.InstanceManagerTypeReplica, "")
	if err != nil {
		return err
	}

	for _, rm := range rmMap {
		log.Infof("Cleaning up the replica manager %v since there is no available disk on this node", rm.Name)
		if err := nc.ds.DeleteInstanceManager(rm.Name); err != nil {
			return err
		}
	}

	return nil
}

func (nc *NodeController) syncInstanceManagers(node *longhorn.Node) error {
	defaultInstanceManagerImage, err := nc.ds.GetSettingValueExisted(types.SettingNameDefaultInstanceManagerImage)
	if err != nil {
		return err
	}

	log := getLoggerForNode(nc.logger, node)

	// Clean up all replica managers if there is no disk on the node
	if len(node.Spec.Disks) == 0 {
		if err := nc.cleanupAllReplicaManagers(node); err != nil {
			return err
		}
	}

	imTypeDataEngines := nc.getImTypeDataEngines(node)

	for imType, dataEngines := range imTypeDataEngines {
		for _, dataEngine := range dataEngines {
			defaultInstanceManagerCreated := false
			imMap, err := nc.ds.ListInstanceManagersByNodeRO(node.Name, imType, dataEngine)
			if err != nil {
				return err
			}
			for _, im := range imMap {
				if im.Labels[types.GetLonghornLabelKey(types.LonghornLabelNode)] != im.Spec.NodeID {
					return fmt.Errorf("instance manager %v nodeID %v is not consistent with the label %v=%v",
						im.Name, im.Spec.NodeID, types.GetLonghornLabelKey(types.LonghornLabelNode), im.Labels[types.GetLonghornLabelKey(types.LonghornLabelNode)])
				}

				runningOrStartingInstanceFound := false
				if im.Status.CurrentState == longhorn.InstanceManagerStateRunning && im.DeletionTimestamp == nil {
					// nolint:all
					for _, instance := range types.ConsolidateInstances(im.Status.InstanceEngines, im.Status.InstanceReplicas, im.Status.Instances) {
						if instance.Status.State == longhorn.InstanceStateRunning || instance.Status.State == longhorn.InstanceStateStarting {
							runningOrStartingInstanceFound = true
							break
						}
					}
				}

				cleanupRequired := true

				if im.Spec.Image == defaultInstanceManagerImage && im.Spec.DataEngine == dataEngine {
					// Create default instance manager if needed.
					defaultInstanceManagerCreated = true
					cleanupRequired = false

					if types.IsDataEngineV2(dataEngine) {
						disabled, err := nc.ds.IsV2DataEngineDisabledForNode(node.Name)
						if err != nil {
							return errors.Wrapf(err, "failed to check if v2 data engine is disabled on node %v", node.Name)
						}
						if disabled && !runningOrStartingInstanceFound {
							log.Infof("Cleaning up instance manager %v since v2 data engine is disabled for node %v", im.Name, node.Name)
							cleanupRequired = true
						}
					}
				} else {
					// Clean up old instance managers if there is no running instance.
					if runningOrStartingInstanceFound {
						cleanupRequired = false
					}

					if im.Status.CurrentState == longhorn.InstanceManagerStateUnknown && im.DeletionTimestamp == nil {
						cleanupRequired = false
						log.Debugf("Skipping cleaning up non-default unknown instance manager %s", im.Name)
					}
				}
				if cleanupRequired {
					log.Infof("Cleaning up the redundant instance manager %v when there is no running/starting instance", im.Name)
					if err := nc.ds.DeleteInstanceManager(im.Name); err != nil {
						return err
					}
				}
			}
			if !defaultInstanceManagerCreated && imType == longhorn.InstanceManagerTypeAllInOne {
				imName, err := types.GetInstanceManagerName(imType, node.Name, defaultInstanceManagerImage, string(dataEngine))
				if err != nil {
					return err
				}
				if types.IsDataEngineV2(dataEngine) {
					disabled, err := nc.ds.IsV2DataEngineDisabledForNode(node.Name)
					if err != nil {
						return errors.Wrapf(err, "failed to check if v2 data engine is disabled on node %v", node.Name)
					}
					if disabled {
						continue
					}
				}

				log.Infof("Creating default instance manager %v, image: %v, dataEngine: %v", imName, defaultInstanceManagerImage, dataEngine)
				_, err = nc.createInstanceManager(node, imName, defaultInstanceManagerImage, imType, dataEngine)
				if err != nil {
					if apierrors.IsAlreadyExists(err) {
						log.WithError(err).Warnf("Deleting instance manager %v because it cannot be obtained by selector labels", imName)
						if err := nc.ds.DeleteInstanceManager(imName); err != nil {
							return err
						}

						nc.enqueueNode(node)
						return nil
					}
					return err
				}
			}
		}
	}
	return nil
}

func (nc *NodeController) createInstanceManager(node *longhorn.Node, imName, imImage string, imType longhorn.InstanceManagerType, dataEngine longhorn.DataEngineType) (*longhorn.InstanceManager, error) {
	instanceManager := &longhorn.InstanceManager{
		ObjectMeta: metav1.ObjectMeta{
			Name: imName,
		},
		Spec: longhorn.InstanceManagerSpec{
			Image:      imImage,
			NodeID:     node.Name,
			Type:       imType,
			DataEngine: dataEngine,
		},
	}

	return nc.ds.CreateInstanceManager(instanceManager)
}

func (nc *NodeController) cleanUpBackingImagesInDisks(node *longhorn.Node) error {
	log := getLoggerForNode(nc.logger, node)

	settingValue, err := nc.ds.GetSettingAsInt(types.SettingNameBackingImageCleanupWaitInterval)
	if err != nil {
		log.WithError(err).Warnf("Failed to get setting %v, won't do cleanup for backing images", types.SettingNameBackingImageCleanupWaitInterval)
		return nil
	}
	waitInterval := time.Duration(settingValue) * time.Minute

	backingImages, err := nc.ds.ListBackingImages()
	if err != nil {
		return err
	}
	for _, bi := range backingImages {
		log := getLoggerForBackingImage(nc.logger, bi).WithField("node", node.Name)
		bids, err := nc.ds.GetBackingImageDataSource(bi.Name)
		if err != nil && !datastore.ErrorIsNotFound(err) {
			log.WithError(err).Warn("Failed to get the backing image data source when cleaning up the images in disks")
			continue
		}
		if bids == nil {
			continue
		}
		existingBackingImage := bi.DeepCopy()
		BackingImageDiskFileCleanup(node, bi, bids, waitInterval, bi.Spec.MinNumberOfCopies)
		if !reflect.DeepEqual(existingBackingImage.Spec, bi.Spec) {
			if _, err := nc.ds.UpdateBackingImage(bi); err != nil {
				log.WithError(err).Warn("Failed to update backing image when cleaning up the images in disks")
				// Requeue the node but do not fail the whole sync function
				nc.enqueueNode(node)
				continue
			}
		}
	}

	return nil
}

func BackingImageDiskFileCleanup(node *longhorn.Node, bi *longhorn.BackingImage, bids *longhorn.BackingImageDataSource, waitInterval time.Duration, minNumberOfCopies int) {
	if bi.Spec.DiskFileSpecMap == nil || bi.Status.DiskLastRefAtMap == nil || !bids.Spec.FileTransferred {
		return
	}

	if minNumberOfCopies < 1 {
		minNumberOfCopies = 1
	}

	var readyDiskFileCount, handlingDiskFileCount, failedDiskFileCount int
	for diskUUID := range bi.Spec.DiskFileSpecMap {
		// Consider non-existing files as pending/handling backing image files.
		fileStatus, exists := bi.Status.DiskFileStatusMap[diskUUID]
		if !exists {
			handlingDiskFileCount++
			continue
		}
		switch fileStatus.State {
		case longhorn.BackingImageStateReadyForTransfer, longhorn.BackingImageStateReady:
			readyDiskFileCount++
		case longhorn.BackingImageStateFailed:
			failedDiskFileCount++
		default:
			handlingDiskFileCount++
		}
	}

	for _, diskStatus := range node.Status.DiskStatus {
		diskUUID := diskStatus.DiskUUID
		if _, exists := bi.Spec.DiskFileSpecMap[diskUUID]; !exists {
			continue
		}
		lastRefAtStr, exists := bi.Status.DiskLastRefAtMap[diskUUID]
		if !exists {
			continue
		}
		lastRefAt, err := util.ParseTime(lastRefAtStr)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to parse LastRefAt timestamp %v for backing image %v", lastRefAtStr, bi.Name)
			continue
		}
		if !time.Now().After(lastRefAt.Add(waitInterval)) {
			continue
		}

		// The cleanup strategy:
		//  1. If there are enough ready files for a backing image, it's fine to do cleanup.
		//  2. If there are no enough ready files, try to retain handling(state empty/pending/starting/in-progress/unknown) files to guarantee the HA requirement.
		//  3. If there are no enough ready & handling files, try to retain failed files to guarantee the HA requirement.
		//  4. If there are no enough files including failed ones, skip cleanup.
		fileStatus, exists := bi.Status.DiskFileStatusMap[diskUUID]
		if !exists {
			fileStatus = &longhorn.BackingImageDiskFileStatus{
				State: "",
			}
		}
		switch fileStatus.State {
		case longhorn.BackingImageStateFailed:
			if minNumberOfCopies >= readyDiskFileCount+handlingDiskFileCount+failedDiskFileCount {
				continue
			}
			failedDiskFileCount--
		case longhorn.BackingImageStateReadyForTransfer, longhorn.BackingImageStateReady:
			if minNumberOfCopies >= readyDiskFileCount {
				continue
			}
			readyDiskFileCount--
		default:
			if minNumberOfCopies >= readyDiskFileCount+handlingDiskFileCount {
				continue
			}
			handlingDiskFileCount--
		}

		logrus.Infof("Cleaning up the unused file in disk %v for backing image %v", diskUUID, bi.Name)
		delete(bi.Spec.DiskFileSpecMap, diskUUID)
	}
}

func (nc *NodeController) createDiskMonitor() (monitor.Monitor, error) {
	if nc.diskMonitor != nil {
		return nc.diskMonitor, nil
	}

	monitor, err := monitor.NewDiskMonitor(nc.logger, nc.ds, nc.controllerID, nc.enqueueNodeForMonitor)
	if err != nil {
		return nil, err
	}

	nc.diskMonitor = monitor

	return monitor, nil
}

func (nc *NodeController) createEnvironmentCheckMonitor() (monitor.Monitor, error) {
	if nc.environmentCheckMonitor != nil {
		return nc.environmentCheckMonitor, nil
	}

	monitor, err := monitor.NewEnvironmentCheckMonitor(nc.logger, nc.ds, nc.controllerID, nc.enqueueNodeForMonitor)
	if err != nil {
		return nil, err
	}

	nc.environmentCheckMonitor = monitor

	return monitor, nil
}

func (nc *NodeController) enqueueNodeForMonitor(key string) {
	nc.queue.Add(key)
}

func (nc *NodeController) syncOrphans(node *longhorn.Node, collectedDataInfo map[string]*monitor.CollectedDiskInfo) error {
	autoDeleteGracePeriod, err := nc.ds.GetSettingAsInt(types.SettingNameOrphanResourceAutoDeletionGracePeriod)
	if err != nil {
		return errors.Wrapf(err, "failed to get setting %v", types.SettingNameOrphanResourceAutoDeletionGracePeriod)
	}

	for diskName, diskInfo := range collectedDataInfo {
		newOrphanedReplicaDataStores, missingOrphanedReplicaDataStores :=
			nc.getNewAndMissingOrphanedReplicaDataStores(diskName, diskInfo.DiskUUID, diskInfo.Path, diskInfo.OrphanedReplicaDataStores)

		if err := nc.createOrphansForReplicaDataStore(node, diskName, diskInfo, newOrphanedReplicaDataStores); err != nil {
			return errors.Wrapf(err, "failed to create orphans for disk %v", diskName)
		}
		if err := nc.deleteOrphansForReplicaDataStore(node, diskName, diskInfo, missingOrphanedReplicaDataStores, autoDeleteGracePeriod); err != nil {
			return errors.Wrapf(err, "failed to delete orphans for disk %v", diskName)
		}
	}

	if node.Spec.EvictionRequested {
		return nc.deleteOrphansForEngineAndReplicaInstances(node)
	}

	return nil
}

func (nc *NodeController) getNewAndMissingOrphanedReplicaDataStores(diskName, diskUUID, diskPath string, replicaDataStores map[string]string) (map[string]string, map[string]string) {
	newOrphanedReplicaDataStores := map[string]string{}
	missingOrphanedReplicaDataStores := map[string]string{}

	// Find out the new/missing orphaned data stores by checking with orphan CRs
	orphanList, err := nc.ds.ListOrphansByNodeRO(nc.controllerID)
	if err != nil {
		nc.logger.WithError(err).Warnf("Failed to list orphans for node %v", nc.controllerID)
		return map[string]string{}, map[string]string{}
	}
	orphanMap := make(map[string]*longhorn.Orphan, len(orphanList))
	for _, o := range orphanList {
		if o.Spec.Type == longhorn.OrphanTypeReplicaData {
			orphanMap[o.Name] = o
		}
	}

	for dataStore := range replicaDataStores {
		orphanName := types.GetOrphanChecksumNameForOrphanedDataStore(nc.controllerID, diskName, diskPath, diskUUID, dataStore)
		if _, ok := orphanMap[orphanName]; !ok {
			newOrphanedReplicaDataStores[dataStore] = ""
		}
	}

	for _, orphan := range orphanMap {
		if orphan.Spec.Parameters[longhorn.OrphanDiskName] != diskName ||
			orphan.Spec.Parameters[longhorn.OrphanDiskUUID] != diskUUID ||
			orphan.Spec.Parameters[longhorn.OrphanDiskPath] != diskPath {
			continue
		}

		dataStore := orphan.Spec.Parameters[longhorn.OrphanDataName]
		if _, ok := replicaDataStores[dataStore]; !ok {
			missingOrphanedReplicaDataStores[dataStore] = ""
		}
	}

	return newOrphanedReplicaDataStores, missingOrphanedReplicaDataStores
}

func (nc *NodeController) deleteOrphansForEngineAndReplicaInstances(node *longhorn.Node) error {
	nc.logger.Infof("Deleting orphans on evicted node %v", node.Name)

	orphans, err := nc.ds.ListOrphansByNodeRO(node.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to list orphans to evict node %v", node.Name)
	}

	multiError := util.NewMultiError()
	for _, orphan := range orphans {
		switch orphan.Spec.Type {
		case longhorn.OrphanTypeEngineInstance, longhorn.OrphanTypeReplicaInstance:
			if err := nc.ds.DeleteOrphan(orphan.Name); err != nil && !datastore.ErrorIsNotFound(err) {
				multiError.Append(util.NewMultiError(fmt.Sprintf("%v: %v", orphan.Name, err)))
			}
		}
	}
	if len(multiError) > 0 {
		return fmt.Errorf("node controller failed to delete instance orphans: %v", multiError.Join())
	}
	return nil
}

func (nc *NodeController) deleteOrphansForReplicaDataStore(node *longhorn.Node, diskName string, diskInfo *monitor.CollectedDiskInfo, missingOrphanedReplicaDataStores map[string]string, autoDeleteGracePeriod int64) error {
	autoDeletionTypes, err := nc.ds.GetSettingOrphanResourceAutoDeletion()
	if err != nil {
		return errors.Wrapf(err, "failed to get %v setting", types.SettingNameOrphanResourceAutoDeletion)
	}
	autoDeleteEnabled, ok := autoDeletionTypes[types.OrphanResourceTypeReplicaData]
	if !ok {
		autoDeleteEnabled = false
	}

	for dataStore := range missingOrphanedReplicaDataStores {
		orphanName := types.GetOrphanChecksumNameForOrphanedDataStore(node.Name, diskName, diskInfo.Path, diskInfo.DiskUUID, dataStore)
		if err := nc.ds.DeleteOrphan(orphanName); err != nil && !datastore.ErrorIsNotFound(err) {
			return errors.Wrapf(err, "failed to delete orphan %v", orphanName)
		}
	}

	orphans, err := nc.ds.ListOrphansRO()
	if err != nil {
		return errors.Wrap(err, "failed to list orphans")
	}

	for _, orphan := range orphans {
		if nc.canDeleteOrphan(orphan, autoDeleteEnabled, autoDeleteGracePeriod) {
			if err := nc.ds.DeleteOrphan(orphan.Name); err != nil && !datastore.ErrorIsNotFound(err) {
				return errors.Wrapf(err, "failed to delete orphan %v", orphan.Name)
			}
		}
	}
	return nil
}

func (nc *NodeController) canDeleteOrphan(orphan *longhorn.Orphan, autoDeleteEnabled bool, autoDeleteGracePeriod int64) bool {
	if orphan.Status.OwnerID != nc.controllerID {
		return false
	}

	dataCleanableCondition := types.GetCondition(orphan.Status.Conditions, longhorn.OrphanConditionTypeDataCleanable)
	if dataCleanableCondition.Status == longhorn.ConditionStatusUnknown {
		return false
	}

	autoDeleteAllowed := false
	if autoDeleteEnabled {
		elapsedTime := time.Since(orphan.CreationTimestamp.Time).Seconds()
		if elapsedTime > float64(autoDeleteGracePeriod) {
			autoDeleteAllowed = true
		}
	}

	// When dataCleanableCondition is false, it means the associated node is not ready, missing or evicted (check updateDataCleanableCondition()).
	// In this case, we can delete the orphan directly because the data is not reachable and no need to keep the orphan resource.
	canDelete := autoDeleteAllowed || dataCleanableCondition.Status == longhorn.ConditionStatusFalse
	if !canDelete {
		nc.logger.Debugf("Orphan %v is not ready to be deleted, autoDeleteAllowed: %v, dataCleanableCondition: %v", orphan.Name, autoDeleteAllowed, dataCleanableCondition.Status)
	}

	return canDelete
}

func (nc *NodeController) createOrphansForReplicaDataStore(node *longhorn.Node, diskName string, diskInfo *monitor.CollectedDiskInfo, newOrphanedReplicaDataStores map[string]string) error {
	for dataStore := range newOrphanedReplicaDataStores {
		if err := nc.createOrphan(node, diskName, dataStore, diskInfo); err != nil && !apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(err, "failed to create orphan for orphaned replica data store %v in disk %v on node %v",
				dataStore, node.Spec.Disks[diskName].Path, node.Name)
		}
	}
	return nil
}

func (nc *NodeController) createOrphan(node *longhorn.Node, diskName, replicaDataStore string, diskInfo *monitor.CollectedDiskInfo) error {
	name := types.GetOrphanChecksumNameForOrphanedDataStore(node.Name, diskName, diskInfo.Path, diskInfo.DiskUUID, replicaDataStore)

	_, err := nc.ds.GetOrphanRO(name)
	if err == nil || (err != nil && !datastore.ErrorIsNotFound(err)) {
		return err
	}

	orphan := &longhorn.Orphan{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: longhorn.OrphanSpec{
			NodeID: node.Name,
			Type:   longhorn.OrphanTypeReplicaData,
			Parameters: map[string]string{
				longhorn.OrphanDataName: replicaDataStore,
				longhorn.OrphanDiskName: diskName,
				longhorn.OrphanDiskUUID: node.Status.DiskStatus[diskName].DiskUUID,
				longhorn.OrphanDiskPath: node.Spec.Disks[diskName].Path,
				longhorn.OrphanDiskType: string(node.Spec.Disks[diskName].Type),
			},
		},
	}

	_, err = nc.ds.CreateOrphan(orphan)

	return err
}

func (nc *NodeController) syncWithDiskMonitor(node *longhorn.Node) (map[string]*monitor.CollectedDiskInfo, error) {
	v, err := nc.diskMonitor.GetCollectedData()
	if err != nil {
		return map[string]*monitor.CollectedDiskInfo{}, err
	}

	collectedDiskInfo := v.(map[string]*monitor.CollectedDiskInfo)
	if matched := isDiskMatched(node, collectedDiskInfo); !matched {
		return map[string]*monitor.CollectedDiskInfo{},
			errors.New("mismatching disks in node resource object and monitor collected data")
	}

	return collectedDiskInfo, nil
}

func (nc *NodeController) syncWithEnvironmentCheckMonitor() ([]longhorn.Condition, error) {
	v, err := nc.environmentCheckMonitor.GetCollectedData()
	if err != nil {
		return []longhorn.Condition{}, err
	}

	conditions, ok := v.([]longhorn.Condition)
	if !ok {
		return []longhorn.Condition{}, errors.New("failed to convert the collected data to conditions")
	}

	return conditions, nil
}

// Check all disks in the same filesystem ID are in ready status
func (nc *NodeController) isDiskIDDuplicatedWithExistingReadyDisk(diskName string, diskInfo map[string]*monitor.CollectedDiskInfo, diskStatusMap map[string]*longhorn.DiskStatus) bool {
	if len(diskInfo) > 1 {
		for otherName := range diskInfo {
			diskReady := types.GetCondition(diskStatusMap[otherName].Conditions, longhorn.DiskConditionTypeReady)

			if otherName != diskName && diskReady.Status == longhorn.ConditionStatusTrue {
				return true
			}
		}
	}

	return false
}

func (nc *NodeController) alignDiskSpecAndStatus(node *longhorn.Node) {
	if node.Status.DiskStatus == nil {
		node.Status.DiskStatus = map[string]*longhorn.DiskStatus{}
	}

	for diskName := range node.Spec.Disks {
		if node.Status.DiskStatus[diskName] == nil {
			node.Status.DiskStatus[diskName] = &longhorn.DiskStatus{}
		}
		diskStatus := node.Status.DiskStatus[diskName]
		if diskStatus.Conditions == nil {
			diskStatus.Conditions = []longhorn.Condition{}
		}
		if diskStatus.ScheduledReplica == nil {
			diskStatus.ScheduledReplica = map[string]int64{}
		}
		if diskStatus.ScheduledBackingImage == nil {
			diskStatus.ScheduledBackingImage = map[string]int64{}
		}
		// When condition are not ready, the old storage data should be cleaned.
		diskStatus.StorageMaximum = 0
		diskStatus.StorageAvailable = 0
		diskStatus.Type = node.Spec.Disks[diskName].Type
		node.Status.DiskStatus[diskName] = diskStatus
	}

	for diskName := range node.Status.DiskStatus {
		if _, exists := node.Spec.Disks[diskName]; !exists {
			diskStatus, ok := node.Status.DiskStatus[diskName]
			if !ok {
				continue
			}

			// Blindly send disk deletion request to instance manager regardless of the disk type,
			// because the disk type is not recorded in the disk status.
			diskInstanceName := diskStatus.DiskName
			if diskInstanceName == "" {
				diskInstanceName = diskName
			}
			if err := nc.deleteDisk(diskStatus.Type, diskInstanceName, diskStatus.DiskUUID, diskStatus.DiskPath, string(diskStatus.DiskDriver)); err != nil {
				nc.logger.WithError(err).Warnf("Failed to delete disk %v", diskInstanceName)
			}
			delete(node.Status.DiskStatus, diskName)
		}
	}
}

func (nc *NodeController) deleteDisk(diskType longhorn.DiskType, diskName, diskUUID, diskPath, diskDriver string) error {
	nc.logger.Infof("Deleting disk %v with diskUUID %v", diskName, diskUUID)

	dataEngine := util.GetDataEngineForDiskType(diskType)

	im, err := nc.ds.GetRunningInstanceManagerByNodeRO(nc.controllerID, dataEngine)
	if err != nil {
		return errors.Wrapf(err, "failed to get running instance manager for data engine %v", dataEngine)
	}

	diskServiceClient, err := engineapi.NewDiskServiceClient(im, nc.logger)
	if err != nil {
		return errors.Wrapf(err, "failed to create disk service client for deleting disk %v", diskName)
	}
	defer diskServiceClient.Close()

	if err := monitor.DeleteDisk(diskType, diskName, diskUUID, diskPath, diskDriver, diskServiceClient); err != nil {
		return errors.Wrapf(err, "failed to delete disk %v", diskName)
	}

	return nil
}

func isReadyDiskFound(diskInfoMap map[string]*monitor.CollectedDiskInfo) bool {
	return len(diskInfoMap) > 0
}

func isDiskMatched(node *longhorn.Node, collectedDiskInfo map[string]*monitor.CollectedDiskInfo) bool {
	if len(node.Spec.Disks) != len(collectedDiskInfo) {
		logrus.Warnf("Number of node disks %v and collected disk info %v are not equal",
			len(node.Spec.Disks), len(collectedDiskInfo))
		return false
	}

	for diskName, diskInfo := range collectedDiskInfo {
		disk, ok := node.Spec.Disks[diskName]
		if !ok {
			logrus.Warnf("Failed to find disk %v in node %v", diskName, node.Name)
			return false
		}

		nodeOrDiskEvicted := node.Spec.EvictionRequested || disk.EvictionRequested
		if nodeOrDiskEvicted != diskInfo.NodeOrDiskEvicted ||
			disk.Path != diskInfo.Path {
			logrus.Warnf("Disk data %v is mismatched with collected data %v for disk %v", disk, diskInfo, diskName)
			return false
		}
	}

	return true
}

func (nc *NodeController) createSnapshotMonitor() (mon monitor.Monitor, err error) {
	defer func() {
		if err == nil {
			err = nc.snapshotMonitor.UpdateConfiguration(map[string]interface{}{})
		}
	}()

	if nc.snapshotMonitor != nil {
		return nc.snapshotMonitor, nil
	}

	mon, err = monitor.NewSnapshotMonitor(nc.logger, nc.ds, nc.controllerID, nc.eventRecorder, nc.snapshotChangeEventQueue, nc.enqueueNodeForMonitor)
	if err != nil {
		return nil, err
	}

	nc.snapshotMonitor = mon

	return mon, nil
}

func (nc *NodeController) syncBackingImageEvictionRequested(node *longhorn.Node) error {
	// preventing periodically list all backingimage.
	if !isNodeOrDisksEvictionRequested(node) {
		return nil
	}
	log := getLoggerForNode(nc.logger, node)

	diskBackingImageMap, err := nc.ds.GetCurrentDiskBackingImageMap()
	if err != nil {
		return err
	}

	type backingImageToSync struct {
		*longhorn.BackingImage
		diskUUID          string
		evictionRequested bool
	}
	backingImagesToSync := []backingImageToSync{}

	var diskFileSpecNotSync = false
	for diskName, diskSpec := range node.Spec.Disks {
		diskStatus := node.Status.DiskStatus[diskName]
		diskUUID := diskStatus.DiskUUID

		requireDiskFileEviction := diskSpec.EvictionRequested || node.Spec.EvictionRequested
		for _, backingImage := range diskBackingImageMap[diskUUID] {
			// trigger or cancel the eviction request on disks
			if diskFileSpec, ok := backingImage.Spec.DiskFileSpecMap[diskUUID]; ok && diskFileSpec.EvictionRequested != requireDiskFileEviction {
				diskFileSpec.EvictionRequested = requireDiskFileEviction
				backingImagesToSync = append(backingImagesToSync, backingImageToSync{backingImage, diskUUID, requireDiskFileEviction})
			} else if !ok {
				log.Infof("Evicting missing disk %s from backing image %s. Will enqueue then resync the node %s", diskUUID, backingImage.Name, node.Name)
				diskFileSpecNotSync = true
			}
		}
	}
	if diskFileSpecNotSync {
		nc.enqueueNodeRateLimited(node)
	}

	for _, backingImageToSync := range backingImagesToSync {
		backingImageLog := log.WithField("backingimage", backingImageToSync.Name).WithField("disk", backingImageToSync.diskUUID)
		eventReason := constant.EventReasonEvictionCanceled    // Default to "EvictionCanceled"
		logMessage := "Cancelling backing image copy eviction" // Default message

		if backingImageToSync.evictionRequested {
			eventReason = constant.EventReasonEvictionUserRequested
			logMessage = "Requesting backing image copy eviction"
		}

		backingImageLog.Infof("%s", logMessage)
		if _, err := nc.ds.UpdateBackingImage(backingImageToSync.BackingImage); err != nil {
			backingImageLog.Warnf("Failed to %s, will enqueue then resync the node", strings.ToLower(logMessage))
			nc.enqueueNodeRateLimited(node)
			continue
		}

		nc.eventRecorder.Eventf(backingImageToSync.BackingImage, corev1.EventTypeNormal, eventReason, "%s from node %v and disk %v", logMessage, node.Spec.Name, backingImageToSync.diskUUID)
	}

	return nil
}

func (nc *NodeController) syncReplicaEvictionRequested(node *longhorn.Node, kubeNode *corev1.Node) error {
	log := getLoggerForNode(nc.logger, node)
	node.Status.AutoEvicting = false
	nodeDrainPolicy, err := nc.ds.GetSettingValueExisted(types.SettingNameNodeDrainPolicy)
	if err != nil {
		return errors.Wrapf(err, "failed to get %v setting", types.SettingNameNodeDrainPolicy)
	}

	type replicaToSync struct {
		*longhorn.Replica
		syncReason string
	}
	replicasToSync := []replicaToSync{}

	for diskName, diskSpec := range node.Spec.Disks {
		diskStatus := node.Status.DiskStatus[diskName]
		for replicaName := range diskStatus.ScheduledReplica {
			replica, err := nc.ds.GetReplica(replicaName)
			if err != nil {
				return err
			}
			shouldEvictReplica, reason, err := nc.shouldEvictReplica(node, kubeNode, &diskSpec, replica,
				nodeDrainPolicy)
			if err != nil {
				return err
			}
			if replica.Spec.EvictionRequested != shouldEvictReplica {
				replica.Spec.EvictionRequested = shouldEvictReplica
				replicasToSync = append(replicasToSync, replicaToSync{replica, reason})
			}

			if replica.Spec.EvictionRequested && !node.Spec.EvictionRequested && !diskSpec.EvictionRequested {
				// We don't consider the node to be auto evicting if eviction was manually requested.
				node.Status.AutoEvicting = true
			}
		}
	}

	for _, replicaToSync := range replicasToSync {
		replicaLog := log.WithField("replica", replicaToSync.Name).WithField("disk", replicaToSync.Spec.DiskID)
		if replicaToSync.Spec.EvictionRequested {
			replicaLog.Infof("Requesting replica eviction")
			if _, err := nc.ds.UpdateReplica(replicaToSync.Replica); err != nil {
				replicaLog.Warn("Failed to request replica eviction, will enqueue then resync node")
				nc.enqueueNodeRateLimited(node)
				continue
			}
			nc.eventRecorder.Eventf(replicaToSync.Replica, corev1.EventTypeNormal, replicaToSync.syncReason, "Requesting replica %v eviction from node %v and disk %v", replicaToSync.Name, node.Spec.Name, replicaToSync.Spec.DiskID)
		} else {
			replicaLog.Infof("Cancelling replica eviction")
			if _, err := nc.ds.UpdateReplica(replicaToSync.Replica); err != nil {
				replicaLog.Warn("Failed to cancel replica eviction, will enqueue then resync node")
				nc.enqueueNodeRateLimited(node)
				continue
			}
			nc.eventRecorder.Eventf(replicaToSync.Replica, corev1.EventTypeNormal, replicaToSync.syncReason, "Cancelling replica %v eviction from node %v and disk %v", replicaToSync.Name, node.Spec.Name, replicaToSync.Spec.DiskID)
		}
	}

	return nil
}

func (nc *NodeController) shouldEvictReplica(node *longhorn.Node, kubeNode *corev1.Node, diskSpec *longhorn.DiskSpec,
	replica *longhorn.Replica, nodeDrainPolicy string) (bool, string, error) {
	// Replica eviction was cancelled on down or deleted nodes in previous implementations. It seems safest to continue
	// this behavior unless we find a reason to change it.
	if isDownOrDeleted, err := nc.ds.IsNodeDownOrDeleted(node.Spec.Name); err != nil {
		return false, "", err
	} else if isDownOrDeleted {
		return false, longhorn.NodeConditionReasonKubernetesNodeNotReady, nil
	}
	if kubeNode == nil {
		return false, longhorn.NodeConditionReasonKubernetesNodeGone, nil
	}

	if node.Spec.EvictionRequested || diskSpec.EvictionRequested {
		return true, constant.EventReasonEvictionUserRequested, nil
	}
	if !kubeNode.Spec.Unschedulable {
		// Node drain policy only takes effect on cordoned nodes.
		return false, constant.EventReasonEvictionCanceled, nil
	}
	if nodeDrainPolicy == string(types.NodeDrainPolicyBlockForEviction) {
		return true, constant.EventReasonEvictionAutomatic, nil
	}
	if nodeDrainPolicy != string(types.NodeDrainPolicyBlockForEvictionIfContainsLastReplica) {
		return false, constant.EventReasonEvictionCanceled, nil
	}

	pdbProtectedHealthyReplicas, err := nc.ds.ListVolumePDBProtectedHealthyReplicasRO(replica.Spec.VolumeName)
	if err != nil {
		return false, "", err
	}
	hasPDBOnAnotherNode := false
	for _, pdbProtectedHealthyReplica := range pdbProtectedHealthyReplicas {
		if pdbProtectedHealthyReplica.Spec.NodeID != replica.Spec.NodeID {
			hasPDBOnAnotherNode = true
			break
		}
	}
	if !hasPDBOnAnotherNode {
		return true, constant.EventReasonEvictionAutomatic, nil
	}

	return false, constant.EventReasonEvictionCanceled, nil
}

func isNodeOrDisksEvictionRequested(node *longhorn.Node) bool {
	if node.Spec.EvictionRequested {
		return true
	}

	for _, diskSpec := range node.Spec.Disks {
		if diskSpec.EvictionRequested {
			return true
		}
	}

	return false
}

func (nc *NodeController) setReadyAndSchedulableConditions(node *longhorn.Node, kubeNode *corev1.Node, managerPods []*corev1.Pod) error {
	nodeReady := true
	nodeReady = nc.setReadyConditionForManagerPod(node, managerPods, nodeReady)
	nodeReady = nc.setReadyConditionForKubeNode(node, kubeNode, nodeReady)
	if nodeReady {
		// Only record true if we did not already record false.
		node.Status.Conditions = types.SetConditionAndRecord(node.Status.Conditions,
			longhorn.NodeConditionTypeReady, longhorn.ConditionStatusTrue,
			"", fmt.Sprintf("Node %v is ready", node.Name),
			nc.eventRecorder, node, corev1.EventTypeNormal)
	}

	disableSchedulingOnCordonedNode, err :=
		nc.ds.GetSettingAsBool(types.SettingNameDisableSchedulingOnCordonedNode)
	if err != nil {
		return errors.Wrapf(err, "failed to get %v setting", types.SettingNameDisableSchedulingOnCordonedNode)
	}
	nc.SetSchedulableCondition(node, kubeNode, disableSchedulingOnCordonedNode)

	return nil
}

func (nc *NodeController) setReadyConditionForManagerPod(node *longhorn.Node, managerPods []*corev1.Pod, nodeReady bool) bool {
	nodeManagerFound := false
	for _, pod := range managerPods {
		if pod.Spec.NodeName == node.Name {
			nodeManagerFound = true
			podConditions := pod.Status.Conditions
			for _, podCondition := range podConditions {
				if podCondition.Type == corev1.PodReady {
					if podCondition.Status != corev1.ConditionTrue || pod.Status.Phase != corev1.PodRunning {
						nodeReady = false
						node.Status.Conditions = types.SetConditionAndRecord(node.Status.Conditions,
							longhorn.NodeConditionTypeReady, longhorn.ConditionStatusFalse,
							string(longhorn.NodeConditionReasonManagerPodDown),
							fmt.Sprintf("Node %v is down: manager pod %v is not running", node.Name, pod.Name),
							nc.eventRecorder, node, corev1.EventTypeWarning)
					}
					break
				}
			}
			break
		}
	}
	if !nodeManagerFound {
		nodeReady = false
		node.Status.Conditions = types.SetConditionAndRecord(node.Status.Conditions,
			longhorn.NodeConditionTypeReady, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonManagerPodMissing),
			fmt.Sprintf("Manager pod is missing: node %v has no manager pod running on it", node.Name),
			nc.eventRecorder, node, corev1.EventTypeWarning)
	}
	return nodeReady
}

func (nc *NodeController) setReadyConditionForKubeNode(node *longhorn.Node, kubeNode *corev1.Node, nodeReady bool) bool {
	kubeConditions := kubeNode.Status.Conditions
	for _, con := range kubeConditions {
		switch con.Type {
		case corev1.NodeReady:
			if con.Status != corev1.ConditionTrue {
				if con.Status == corev1.ConditionFalse &&
					time.Since(con.LastTransitionTime.Time) < ignoreKubeletNotReadyTime {
					// When kubelet restarts, it briefly reports Ready == False. Responding too quickly can cause
					// undesirable churn. See https://github.com/longhorn/longhorn/issues/7302 for an example.
					nc.logger.Warnf("Ignoring %v == %v condition due to %v until %v", corev1.NodeReady, con.Status,
						con.Reason, con.LastTransitionTime.Add(ignoreKubeletNotReadyTime))
				} else {
					nodeReady = false
					node.Status.Conditions = types.SetConditionAndRecord(node.Status.Conditions,
						longhorn.NodeConditionTypeReady, longhorn.ConditionStatusFalse,
						string(longhorn.NodeConditionReasonKubernetesNodeNotReady), // This one.
						fmt.Sprintf("Kubernetes node %v not ready: %v", node.Name, con.Reason),
						nc.eventRecorder, node, corev1.EventTypeWarning)
				}
			}
		case corev1.NodeDiskPressure,
			corev1.NodePIDPressure,
			corev1.NodeMemoryPressure,
			corev1.NodeNetworkUnavailable:
			if con.Status == corev1.ConditionTrue {
				nodeReady = false
				node.Status.Conditions = types.SetConditionAndRecord(node.Status.Conditions,
					longhorn.NodeConditionTypeReady, longhorn.ConditionStatusFalse,
					string(longhorn.NodeConditionReasonKubernetesNodePressure),
					fmt.Sprintf("Kubernetes node %v has pressure: %v, %v", node.Name, con.Reason, con.Message),
					nc.eventRecorder, node, corev1.EventTypeWarning)
			}
		}
	}
	return nodeReady
}

// Update node condition based on DisableSchedulingOnCordonedNode setting and Kubernetes node status.
func (nc *NodeController) SetSchedulableCondition(node *longhorn.Node, kubeNode *corev1.Node,
	disableSchedulingOnCordonedNode bool) {
	kubeSpec := kubeNode.Spec
	if disableSchedulingOnCordonedNode &&
		kubeSpec.Unschedulable {
		node.Status.Conditions =
			types.SetConditionAndRecord(node.Status.Conditions,
				longhorn.NodeConditionTypeSchedulable,
				longhorn.ConditionStatusFalse,
				string(longhorn.NodeConditionReasonKubernetesNodeCordoned),
				fmt.Sprintf("Node %v is cordoned", node.Name),
				nc.eventRecorder, node,
				corev1.EventTypeNormal)
	} else {
		node.Status.Conditions =
			types.SetConditionAndRecord(node.Status.Conditions,
				longhorn.NodeConditionTypeSchedulable,
				longhorn.ConditionStatusTrue,
				"",
				"",
				nc.eventRecorder, node,
				corev1.EventTypeNormal)
	}
}

func (nc *NodeController) clearDelinquentLeasesIfNodeNotReady(node *longhorn.Node) error {
	enabled, err := nc.ds.GetSettingAsBool(types.SettingNameRWXVolumeFastFailover)
	if err != nil {
		return errors.Wrapf(err, "failed to get setting %v", types.SettingNameRWXVolumeFastFailover)
	}
	if !enabled {
		return nil
	}

	isDownOrDeleted, err := nc.ds.IsNodeDownOrDeleted(node.Name)
	if err != nil {
		return errors.Wrapf(err, "failed to check IsNodeDownOrDeleted, node=%v", node.Name)
	}
	if !isDownOrDeleted {
		return nil
	}

	sms, err := nc.ds.ListShareManagersRO()
	if err != nil {
		return errors.Wrap(err, "failed to list share managers")
	}
	var storedError error
	for _, sm := range sms {
		// It's tempting to filter by sm.Status.OwnerID here, but don't.  It's already been modified to a new node to handle the fast failover.
		// Anyway, we just need its name.  Share manager name is volume name is lease name.
		err = nc.ds.ClearDelinquentAndStaleStateIfVolumeIsDelinquent(sm.Name, node.Name)
		if err != nil {
			nc.logger.WithError(err).Warnf("failed to clear delinquent lease for volume %v, node %v", sm.Name, node.Name)
			if storedError == nil {
				storedError = errors.Wrapf(err, "failed to clear delinquent lease for volume %v, node %v", sm.Name, node.Name)
			}
		}
	}

	return storedError
}
