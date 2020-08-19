package manager

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/pkg/errors"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

type VolumeManager struct {
	ds *datastore.DataStore

	currentNodeID string
	sb            *SupportBundle
}

const (
	MaxRecurringJobRetain = 50
)

// NewVolumeManager returns a new VolumeManager
func NewVolumeManager(currentNodeID string, ds *datastore.DataStore) *VolumeManager {
	return &VolumeManager{
		ds: ds,

		currentNodeID: currentNodeID,
	}
}

// GetCurrentNodeID returns VolumeManager currentNodeID
func (m *VolumeManager) GetCurrentNodeID() string {
	return m.currentNodeID
}

// Node2APIAddress returns the node IP with default API port where manager pod
// is running.
// Returns when the given node name not exist
func (m *VolumeManager) Node2APIAddress(nodeID string) (string, error) {
	nodeIPMap, err := m.ds.GetManagerNodeIPMap()
	if err != nil {
		return "", err
	}
	ip, exists := nodeIPMap[nodeID]
	if !exists {
		return "", fmt.Errorf("cannot find longhorn manager on node %v", nodeID)
	}
	return types.GetAPIServerAddressFromIP(ip), nil
}

// List returns a single object contains all the longhorn Volume
func (m *VolumeManager) List() (map[string]*longhorn.Volume, error) {
	return m.ds.ListVolumes()
}

// sortKeys accepts a map with string keys and returns a sorted slice of keys
func sortKeys(mapObj interface{}) ([]string, error) {
	if mapObj == nil {
		return []string{}, fmt.Errorf("BUG: mapObj was nil")
	}
	m := reflect.ValueOf(mapObj)
	if m.Kind() != reflect.Map {
		return []string{}, fmt.Errorf("BUG: expected map, got %v", m.Kind())
	}

	keys := make([]string, m.Len())
	for i, key := range m.MapKeys() {
		if key.Kind() != reflect.String {
			return []string{}, fmt.Errorf("BUG: expect map[string]interface{}, got map[%v]interface{}", key.Kind())
		}
		keys[i] = key.String()
	}
	sort.Strings(keys)
	return keys, nil
}

// ListSorted returns a single sorted object contains all the longhorn volumes
func (m *VolumeManager) ListSorted() ([]*longhorn.Volume, error) {
	volumeMap, err := m.List()
	if err != nil {
		return []*longhorn.Volume{}, err
	}

	volumes := make([]*longhorn.Volume, len(volumeMap))
	volumeNames, err := sortKeys(volumeMap)
	if err != nil {
		return []*longhorn.Volume{}, err
	}
	for i, volumeName := range volumeNames {
		volumes[i] = volumeMap[volumeName]
	}
	return volumes, nil
}

// Get gets the Volume for the given volume name
func (m *VolumeManager) Get(vName string) (*longhorn.Volume, error) {
	return m.ds.GetVolume(vName)
}

// GetEngines returns single object contains all Longhorn Engines with label marked
// for the given volume name
func (m *VolumeManager) GetEngines(vName string) (map[string]*longhorn.Engine, error) {
	return m.ds.ListVolumeEngines(vName)
}

// GetEnginesSorted returns a sorted list of Longhorn Engine with label marked
// for the given volume name
func (m *VolumeManager) GetEnginesSorted(vName string) ([]*longhorn.Engine, error) {
	engineMap, err := m.ds.ListVolumeEngines(vName)
	if err != nil {
		return []*longhorn.Engine{}, err
	}

	engines := make([]*longhorn.Engine, len(engineMap))
	engineNames, err := sortKeys(engineMap)
	if err != nil {
		return []*longhorn.Engine{}, err
	}
	for i, engineName := range engineNames {
		engines[i] = engineMap[engineName]
	}
	return engines, nil
}

// GetReplicas returns a single object contains all Replica with label marked
// for the given volume name
func (m *VolumeManager) GetReplicas(vName string) (map[string]*longhorn.Replica, error) {
	return m.ds.ListVolumeReplicas(vName)
}

// GetReplicasSorted returns a sorted list of Replica with label marked
// for the given volume name
func (m *VolumeManager) GetReplicasSorted(vName string) ([]*longhorn.Replica, error) {
	replicaMap, err := m.ds.ListVolumeReplicas(vName)
	if err != nil {
		return []*longhorn.Replica{}, err
	}

	replicas := make([]*longhorn.Replica, len(replicaMap))
	replicaNames, err := sortKeys(replicaMap)
	if err != nil {
		return []*longhorn.Replica{}, err
	}
	for i, replicaName := range replicaNames {
		replicas[i] = replicaMap[replicaName]
	}
	return replicas, nil
}

func (m *VolumeManager) getDefaultReplicaCount() (int, error) {
	c, err := m.ds.GetSettingAsInt(types.SettingNameDefaultReplicaCount)
	if err != nil {
		return 0, err
	}
	return int(c), nil
}

// Create the Longhorn Volume resources for the given name and VolumeSpec.
// This uses the backup volume size if the given volume is fromBackup.
// This rounds the volume size to 4096.
// This ensures volume replica is not 0.
// And returns error when:
// * the name contains charactors that is not alphabetic, numberic, "_", ".", "-".
// * the name starts with charactors that is not alphabetic or numberic. 
// * the EngineImage state is not ready
// * the frondend type is not blockdev or iscsi
// * the MountPropagation is disabled on any of the nodes
// * Invalid recurring Jobs
func (m *VolumeManager) Create(name string, spec *types.VolumeSpec) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to create volume %v", name)
		if err != nil {
			logrus.Errorf("manager: unable to create volume %v: %+v: %v", name, spec, err)
		}
	}()

	if !util.ValidateName(name) {
		return nil, fmt.Errorf("invalid name %v", name)
	}

	size := spec.Size
	if spec.FromBackup != "" {
		backupTarget, err := GenerateBackupTarget(m.ds)
		if err != nil {
			return nil, err
		}
		backup, err := backupTarget.GetBackup(spec.FromBackup)
		if err != nil {
			return nil, fmt.Errorf("cannot get backup %v: %v", spec.FromBackup, err)
		}
		logrus.Infof("Override size of volume %v to %v because it's from backup", name, backup.VolumeSize)

		// formalize the final size to the unit in bytes
		size, err = util.ConvertSize(backup.VolumeSize)
		if err != nil {
			return nil, fmt.Errorf("get invalid size for volume %v: %v", backup.VolumeSize, err)
		}
	}

	// make sure it's multiples of 4096
	size = util.RoundUpSize(size)

	if spec.NumberOfReplicas == 0 {
		spec.NumberOfReplicas, err = m.getDefaultReplicaCount()
		if err != nil {
			return nil, errors.Wrap(err, "BUG: cannot get valid number for setting default replica count")
		}
		logrus.Infof("Use the default number of replicas %v", spec.NumberOfReplicas)
	}
	defaultEngineImage, err := m.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if defaultEngineImage == "" {
		return nil, fmt.Errorf("BUG: Invalid empty Setting.EngineImage")
	}
	if err := m.CheckEngineImageReadiness(defaultEngineImage); err != nil {
		return nil, errors.Wrapf(err, "cannot create volume with image %v", defaultEngineImage)
	}

	if !spec.Standby {
		if spec.Frontend != types.VolumeFrontendBlockDev && spec.Frontend != types.VolumeFrontendISCSI {
			return nil, fmt.Errorf("invalid volume frontend specified: %v", spec.Frontend)
		}
	}

	if spec.BaseImage != "" {
		nodes, err := m.ListNodes()
		if err != nil {
			return nil, fmt.Errorf("couldn't list nodes")
		}
		for _, node := range nodes {
			conditions := types.GetCondition(node.Status.Conditions, types.NodeConditionTypeMountPropagation)
			if conditions.Status != types.ConditionStatusTrue {
				return nil, fmt.Errorf("cannot support BaseImage, node doesn't support mount propagation: %v", node)
			}
		}
	}

	if err := m.validateRecurringJobs(spec.RecurringJobs); err != nil {
		return nil, err
	}

	v = &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: types.VolumeSpec{
			Size:                size,
			Frontend:            spec.Frontend,
			EngineImage:         defaultEngineImage,
			FromBackup:          spec.FromBackup,
			NumberOfReplicas:    spec.NumberOfReplicas,
			StaleReplicaTimeout: spec.StaleReplicaTimeout,
			BaseImage:           spec.BaseImage,
			RecurringJobs:       spec.RecurringJobs,
			Standby:             spec.Standby,
			DiskSelector:        spec.DiskSelector,
			NodeSelector:        spec.NodeSelector,
		},
	}
	v, err = m.ds.CreateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Created volume %v: %+v", v.Name, v.Spec)
	return v, nil
}

// Delete Longhorn Volume for the given name
func (m *VolumeManager) Delete(name string) error {
	if err := m.ds.DeleteVolume(name); err != nil {
		return err
	}
	logrus.Debugf("Deleted volume %v", name)
	return nil
}

// Attach updates the Volume for the given nodeID and disableFrontend.
// Returns error when:
// * the Volume state is not detached.
// * the EngineImage state is not ready.
// * the Volume condition is not scheduled.
// * the Volume condition is restore.
// * the Volume NodeID is not empty and does not match the given nodeID
func (m *VolumeManager) Attach(name, nodeID string, disableFrontend bool) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to attach volume %v to %v", name, nodeID)
	}()

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}
	if v.Status.State != types.VolumeStateDetached {
		return nil, fmt.Errorf("invalid state to attach %v: %v", name, v.Status.State)
	}
	if err := m.CheckEngineImageReadiness(v.Spec.EngineImage); err != nil {
		return nil, errors.Wrapf(err, "cannot attach volume %v with image %v", v.Name, v.Spec.EngineImage)
	}

	scheduleCondition := types.GetCondition(v.Status.Conditions, types.VolumeConditionTypeScheduled)
	if scheduleCondition.Status != types.ConditionStatusTrue {
		return nil, fmt.Errorf("volume %v not scheduled", name)
	}
	restoreCondition := types.GetCondition(v.Status.Conditions, types.VolumeConditionTypeRestore)
	if restoreCondition.Status == types.ConditionStatusTrue {
		return nil, fmt.Errorf("volume %v is restoring data", name)
	}

	// already desired to be attached
	if v.Spec.NodeID != "" {
		if v.Spec.NodeID != nodeID {
			return nil, fmt.Errorf("Node to be attached %v is different from previous spec %v", nodeID, v.Spec.NodeID)
		}
		return v, nil
	}
	v.Spec.NodeID = nodeID
	v.Spec.DisableFrontend = disableFrontend

	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Attaching volume %v to %v with disableFrontend set %v", v.Name, v.Spec.NodeID, disableFrontend)
	return v, nil
}

// Detach updates Volume NodeID to "" and DisableFrontend to false for the
// given name.
// Returns error when the Volume state is not attached and attaching
func (m *VolumeManager) Detach(name string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to detach volume %v", name)
	}()

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}
	if v.Status.State != types.VolumeStateAttached && v.Status.State != types.VolumeStateAttaching {
		return nil, fmt.Errorf("invalid state to detach %v: %v", v.Name, v.Status.State)
	}

	oldNodeID := v.Spec.NodeID
	if oldNodeID == "" {
		return v, nil
	}

	v.Spec.NodeID = ""
	v.Spec.DisableFrontend = false

	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Detaching volume %v from %v", v.Name, oldNodeID)
	return v, nil
}

// Salvage updates the Volume with empty NodeID for the given volume name,
// and updates Replicas with empty FaultAt for the given replica name.
// Returns error when:
// * the Volume state is not detached
// * the Volume state robustness is not false
// * the given replica name's Replica VolumeName not equal to the Volume name
func (m *VolumeManager) Salvage(volumeName string, replicaNames []string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to salvage volume %v", volumeName)
	}()

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}
	if v.Status.State != types.VolumeStateDetached {
		return nil, fmt.Errorf("invalid volume state to salvage: %v", v.Status.State)
	}
	if v.Status.Robustness != types.VolumeRobustnessFaulted {
		return nil, fmt.Errorf("invalid robustness state to salvage: %v", v.Status.Robustness)
	}
	v.Spec.NodeID = ""
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}

	for _, name := range replicaNames {
		r, err := m.ds.GetReplica(name)
		if err != nil {
			return nil, err
		}
		if r.Spec.VolumeName != v.Name {
			return nil, fmt.Errorf("replica %v doesn't belong to volume %v", r.Name, v.Name)
		}
		if r.Spec.FailedAt == "" {
			// already updated, ignore it for idempotency
			continue
		}
		r.Spec.FailedAt = ""
		if _, err := m.ds.UpdateReplica(r); err != nil {
			return nil, err
		}
	}

	logrus.Debugf("Salvaged replica %+v for volume %v", replicaNames, v.Name)
	return v, nil
}

// Activate updates the Volume Spec.Standby to false and Spec.Frontend for
// the given frontend and volume name.
// Returns error when:
// * the Volume status is not standby
// * the Volume spec is not standby
// * the given frontend is not blockdev and iscsi
func (m *VolumeManager) Activate(volumeName string, frontend string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to activate volume %v", volumeName)
	}()

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}

	if !v.Status.IsStandby {
		return nil, fmt.Errorf("volume %v is already in active mode", v.Name)
	}
	if !v.Spec.Standby {
		return nil, fmt.Errorf("volume %v is being activated", v.Name)
	}

	if frontend != string(types.VolumeFrontendBlockDev) && frontend != string(types.VolumeFrontendISCSI) {
		return nil, fmt.Errorf("invalid frontend %v", frontend)
	}

	// ListBackupVolumes() will trigger the update for LastBackup
	_, err = m.ListBackupVolumes()
	if err != nil {
		logrus.Warnf("failed to update LastBackup and backup volume list before activating standby volume %v: %v", volumeName, err)
	}

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}

	var engine *longhorn.Engine
	es, err := m.ds.ListVolumeEngines(v.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to list engines for volume %v: %v", v.Name, err)
	}
	if len(es) != 1 {
		return nil, fmt.Errorf("found more than 1 engines for volume %v", v.Name)
	}
	for _, e := range es {
		engine = e
	}

	if v.Status.LastBackup != engine.Status.LastRestoredBackup || engine.Spec.RequestedBackupRestore != engine.Status.LastRestoredBackup {
		logrus.Infof("Standby volume %v will be activated after finishing restoration, "+
			"backup volume's latest backup: %v, "+
			"engine requested backup restore: %v, engine last restored backup: %v",
			v.Name, v.Status.LastBackup, engine.Spec.RequestedBackupRestore, engine.Status.LastRestoredBackup)
	}

	v.Spec.Frontend = types.VolumeFrontend(frontend)
	v.Spec.Standby = false
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("Activating volume %v with frontend %v", v.Name, frontend)
	return v, nil
}

// Expand updates the Volume Spec.DisableFrontend to true and Spec.Size for the
// given volume size and volume name.
// Returns error when:
// the Volume state is not detached
// the Volume condition is not scheduled
// the Volume size is not smaller than the given size
func (m *VolumeManager) Expand(volumeName string, size int64) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to expand volume %v", volumeName)
	}()

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}

	if v.Status.State != types.VolumeStateDetached {
		return nil, fmt.Errorf("invalid volume state to expand: %v", v.Status.State)
	}

	if types.GetCondition(v.Status.Conditions, types.VolumeConditionTypeScheduled).Status != types.ConditionStatusTrue {
		return nil, fmt.Errorf("cannot expand volume before replica scheduling success")
	}

	if v.Spec.Size >= size {
		return nil, fmt.Errorf("cannot expand volume %v with current size %v to a smaller or the same size %v", v.Name, v.Spec.Size, size)
	}
	v.Spec.Size = size

	// Support off-line expansion only.
	v.Spec.DisableFrontend = true

	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("Expanding volume %v to size %v", v.Name, size)
	return v, nil
}

// CancelExpansion updates the Volume size to the current Engine size for the
// given volume name.
// Returns error when:
// * the Volume status does not have ExpansionRequired
// * the Volume status is standby
// * more than 1 Engine found for the Volume name
// * the Engine status is expanding
// * the Engine current size equals to the Volume size
func (m *VolumeManager) CancelExpansion(volumeName string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to cancel expansion for volume %v", volumeName)
	}()

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}
	if !v.Status.ExpansionRequired {
		return nil, fmt.Errorf("volume expansion is not started")
	}
	if v.Status.IsStandby {
		return nil, fmt.Errorf("canceling expansion for standby volume is not supported")
	}

	var engine *longhorn.Engine
	es, err := m.ds.ListVolumeEngines(v.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to list engines for volume %v: %v", v.Name, err)
	}
	if len(es) != 1 {
		return nil, fmt.Errorf("found more than 1 engines for volume %v", v.Name)
	}
	for _, e := range es {
		engine = e
	}

	if engine.Status.IsExpanding {
		return nil, fmt.Errorf("the engine expansion is in progress")
	}
	if engine.Status.CurrentSize == v.Spec.Size {
		return nil, fmt.Errorf("the engine expansion is already complete")
	}

	v.Spec.Size = engine.Status.CurrentSize
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("Canceling expansion for volume %v", v.Name)
	return v, nil
}

// UpdateRecurringJobs updates the Volume.Spec.RecurringJob for the given jobs.
// Returns error when the given jobs are invalid or has duplicates
func (m *VolumeManager) UpdateRecurringJobs(volumeName string, jobs []types.RecurringJob) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to update volume recurring jobs for %v", volumeName)
	}()

	if err = m.validateRecurringJobs(jobs); err != nil {
		return nil, err
	}

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}

	v.Spec.RecurringJobs = jobs
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Updated volume %v recurring jobs to %+v", v.Name, v.Spec.RecurringJobs)
	return v, nil
}

func (m *VolumeManager) checkDuplicateJobs(recurringJobs []types.RecurringJob) error {
	exist := map[string]bool{}
	for _, job := range recurringJobs {
		if _, ok := exist[job.Name]; ok {
			return fmt.Errorf("duplicate job %v in recurringJobs %v", job.Name, recurringJobs)
		}
		exist[job.Name] = true
	}
	return nil
}

// validateRecurringJobs checkes for the given RecurringJob
// Returns error when:
// * job.[Cron, Task, Name] is empty
// * job.Retain equals to 0
// * job.Cron contains invalid cron format
// * job.Name length is more than MaximumJobNameSize
// * invalid job.Labels
// * has duplicated job
// * the total of the given jobs are more than the MaxRecurringJobRetain
func (m *VolumeManager) validateRecurringJobs(jobs []types.RecurringJob) error {
	if jobs == nil {
		return nil
	}

	totalJobRetainCount := 0
	for _, job := range jobs {
		if job.Cron == "" || job.Task == "" || job.Name == "" || job.Retain == 0 {
			return fmt.Errorf("invalid job %+v", job)
		}
		if _, err := cron.ParseStandard(job.Cron); err != nil {
			return fmt.Errorf("invalid cron format(%v): %v", job.Cron, err)
		}
		if len(job.Name) > types.MaximumJobNameSize {
			return fmt.Errorf("job name %v is too long, must be %v characters or less", job.Name, types.MaximumJobNameSize)
		}
		if job.Labels != nil {
			if _, err := util.ValidateSnapshotLabels(job.Labels); err != nil {
				return err
			}
		}
		totalJobRetainCount += job.Retain
	}

	if err := m.checkDuplicateJobs(jobs); err != nil {
		return err
	}
	if totalJobRetainCount > MaxRecurringJobRetain {
		return fmt.Errorf("Job Can't retain more than %d snapshots", MaxRecurringJobRetain)
	}
	return nil
}

// DeleteReplica deletes Replica for the given volume name and replica name.
// Returns error when:
// * the given replica name not exist in Replica
// * no other heathy Replica exist for Volume
func (m *VolumeManager) DeleteReplica(volumeName, replicaName string) error {
	hasHealthyReplicas := false
	rs, err := m.ds.ListVolumeReplicas(volumeName)
	if err != nil {
		return err
	}
	if _, exists := rs[replicaName]; !exists {
		return fmt.Errorf("cannot find replica %v of volume %v", replicaName, volumeName)
	}
	for _, r := range rs {
		if r.Name == replicaName {
			continue
		}
		if r.Spec.FailedAt == "" && r.Spec.HealthyAt != "" {
			hasHealthyReplicas = true
			break
		}
	}
	if !hasHealthyReplicas {
		return fmt.Errorf("no other healthy replica available, cannot delete replica %v since it may still contain data for recovery", replicaName)
	}
	if err := m.ds.DeleteReplica(replicaName); err != nil {
		return err
	}
	logrus.Debugf("Deleted replica %v of volume %v", replicaName, volumeName)
	return nil
}

// GetManagerNodeIPMap returns and object with PodIP mapped to the NodeName.
// Returns error when same node name exist in more than 1 Pod
func (m *VolumeManager) GetManagerNodeIPMap() (map[string]string, error) {
	podList, err := m.ds.ListManagerPods()
	if err != nil {
		return nil, err
	}

	nodeIPMap := map[string]string{}
	for _, pod := range podList {
		if nodeIPMap[pod.Spec.NodeName] != "" {
			return nil, fmt.Errorf("multiple managers on the node %v", pod.Spec.NodeName)
		}
		nodeIPMap[pod.Spec.NodeName] = pod.Status.PodIP
	}
	return nodeIPMap, nil
}

// EngineUpgrade updates the Volume.Spec.EngineImage to the given image and verifies
// the update.
// Returns error when:
// * the EngineImage status is not ready
// * the Volume.Spec.EngineImage already is the given image
// * the Volume.Spec.EngineImage and the given image not equal to the Volume.Status.CurrentImage
// * the Volume state is attached, the state is standby and robustness is not healthy
// * the Volume.Status.Current image not updated
func (m *VolumeManager) EngineUpgrade(volumeName, image string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "cannot upgrade engine for volume %v using image %v", volumeName, image)
	}()
	if err := m.CheckEngineImageReadiness(image); err != nil {
		return nil, err
	}

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}

	if v.Spec.EngineImage == image {
		return nil, fmt.Errorf("upgrading in process for volume %v engine image from %v to %v already",
			v.Name, v.Status.CurrentImage, v.Spec.EngineImage)
	}

	if v.Spec.EngineImage != v.Status.CurrentImage && image != v.Status.CurrentImage {
		return nil, fmt.Errorf("upgrading in process for volume %v engine image from %v to %v, cannot upgrade to another engine image",
			v.Name, v.Status.CurrentImage, v.Spec.EngineImage)
	}

	// TODO: Rebuild is not supported for old DR volumes and the handling of a degraded DR volume live upgrade will get stuck.
	//  Hence the live upgrade should be prevented in API level. After all old DR volumes are gone, this check can be removed.
	if v.Status.State == types.VolumeStateAttached && v.Status.IsStandby && v.Status.Robustness != types.VolumeRobustnessHealthy {
		return nil, fmt.Errorf("cannot do live upgrade for a non-healthy DR volume %v", v.Name)
	}

	oldImage := v.Spec.EngineImage
	v.Spec.EngineImage = image

	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	if image != v.Status.CurrentImage {
		logrus.Debugf("Upgrading volume %v engine image from %v to %v", v.Name, oldImage, v.Spec.EngineImage)
	} else {
		logrus.Debugf("Rolling back volume %v engine image to %v", v.Name, v.Status.CurrentImage)
	}

	return v, nil
}

// UpdateReplicaCount updates the Volume.Spec.NumberOfReplicas for the given number.
// Returns error when:
// * the given count exceeds 1~20
// * the Volume NodeID is empty
// * the Volume state is not attached
// * the Volume EngineImage not equal to its current image
func (m *VolumeManager) UpdateReplicaCount(name string, count int) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to update replica count for volume %v", name)
	}()

	if err := types.ValidateReplicaCount(count); err != nil {
		return nil, err
	}

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}

	if v.Spec.NodeID == "" || v.Status.State != types.VolumeStateAttached {
		return nil, fmt.Errorf("invalid volume state to update replica count%v", v.Status.State)
	}
	if v.Spec.EngineImage != v.Status.CurrentImage {
		return nil, fmt.Errorf("upgrading in process, cannot update replica count")
	}
	oldCount := v.Spec.NumberOfReplicas
	v.Spec.NumberOfReplicas = count

	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Updated volume %v replica count from %v to %v", v.Name, oldCount, v.Spec.NumberOfReplicas)
	return v, nil
}
