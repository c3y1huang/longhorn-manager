package controller

import (
	// "encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/utils/pointer"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	typedv1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	// "github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/constant"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	SampleNamePrefix = "longhorn-sample-"

	SampleBackoffLimit = 3

	SampleMsgRequeueNextPhaseFmt = "Requeue %v for next phase: %v"
)

type sampleRecordType string

const (
	sampleRecordTypeError  = sampleRecordType("error")
	sampleRecordTypeNone   = sampleRecordType("")
	sampleRecordTypeNormal = sampleRecordType("normal")
)

type sampleRecord struct {
	nextState longhorn.SampleState

	recordType sampleRecordType
	message    string
	reason     string
}

type SampleController struct {
	*baseController

	namespace string

	controllerID   string
	managerImage   string
	serviceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced
}

func NewSampleController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace, controllerID, serviceAccount, managerImage string,
) *SampleController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&typedv1core.EventSinkImpl{Interface: typedv1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	c := &SampleController{
		baseController: newBaseController("longhorn-sample", logger),

		namespace:      namespace,
		controllerID:   controllerID,
		managerImage:   managerImage,
		serviceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-sample-controller"}),

		ds: ds,
	}

	ds.SampleInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.enqueue,
		UpdateFunc: func(old, cur interface{}) { c.enqueue(cur) },
		DeleteFunc: c.enqueue,
	}, 0)
	c.cacheSyncs = append(c.cacheSyncs, ds.SampleInformer.HasSynced)

	return c
}

func (c *SampleController) enqueue(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	c.queue.Add(key)
}

func (c *SampleController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	logrus.Infof("Starting Longhorn Stress Job controller")
	defer logrus.Infof("Shut down Longhorn Stress Job controller")

	if !cache.WaitForNamedCacheSync("longhorn sample", stopCh, c.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (c *SampleController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *SampleController) processNextWorkItem() bool {
	key, quit := c.queue.Get()

	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.syncSample(key.(string))
	c.handleErr(err, key)

	return true
}

func (c *SampleController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	if c.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn sample %v: %v", key, err)
		c.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn sample %v out of the queue: %v", key, err)
	c.queue.Forget(key)
}

func getLoggerForSample(logger logrus.FieldLogger, sample *longhorn.Sample) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"sample": sample.Name,
		},
	)
}

func (c *SampleController) syncSample(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync sample for %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != c.namespace {
		return nil
	}

	sample, err := c.ds.GetSample(name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}

	log := getLoggerForSample(c.logger, sample)

	if !c.isResponsibleFor(sample) {
		return nil
	}
	if sample.Status.OwnerID != c.controllerID {
		sample.Status.OwnerID = c.controllerID
		_, err = c.ds.UpdateSampleStatus(sample)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("Picked up by Sample Controller %v", c.controllerID)
		return
	}

	record := &sampleRecord{}
	existingSample := sample.DeepCopy()
	defer c.handleStatusUpdate(record, sample, existingSample, err, log)

	if !sample.DeletionTimestamp.IsZero() &&
		sample.Status.State != longhorn.SampleStateDeleting {
		c.updateRecord(record,
			sampleRecordTypeNormal, longhorn.SampleStateDeleting,
			"", "",
		)
		return
	}

	switch sample.Status.State {
	case longhorn.SampleStateNone:
		_, err := c.ds.UpdateSample(sample)
		if err != nil {
			return err
		}
		c.updateRecord(record,
			sampleRecordTypeNormal, longhorn.SampleStateInitialized,
			"", "",
		)

	case longhorn.SampleStateInitialized:
		if sample.Status.GuaranteedInstanceManagerCPU == nil {
			sample.Status.GuaranteedInstanceManagerCPU = make(map[longhorn.InstanceManagerType]string)
		}
		// Instance manager CPU setting
		guaranteedInstanceManagerCPU, err := c.ds.GetSetting(types.SettingNameGuaranteedInstanceManagerCPU)
		if err != nil {
			if !types.ErrorIsNotSupport(err) {
				log.WithError(err).Warnf("Failed to get setting %v", types.SettingNameGuaranteedInstanceManagerCPU)
				return err
			}

			// Engine manager CPU setting
			guaranteedEngineManagerCPU, err := c.ds.GetSetting(types.SettingNameGuaranteedEngineManagerCPU)
			if err != nil {
				log.WithError(err).Warnf("Failed to get setting %v", types.SettingNameGuaranteedEngineManagerCPU)
				return err
			}
			sample.Status.GuaranteedInstanceManagerCPU[longhorn.InstanceManagerTypeEngine] = guaranteedEngineManagerCPU.Value

			// Replica manager CPU setting
			guaranteedReplicaManagerCPU, err := c.ds.GetSetting(types.SettingNameGuaranteedReplicaManagerCPU)
			if err != nil {
				log.WithError(err).Warnf("Failed to get setting %v", types.SettingNameGuaranteedReplicaManagerCPU)
				return err
			}
			sample.Status.GuaranteedInstanceManagerCPU[longhorn.InstanceManagerTypeReplica] = guaranteedReplicaManagerCPU.Value

			// Instance manager CPU setting
			sample.Status.GuaranteedInstanceManagerCPU[longhorn.InstanceManagerTypeAllInOne] = fmt.Sprintf("%v+%v", guaranteedEngineManagerCPU.Value, guaranteedReplicaManagerCPU.Value)

		} else {
			sample.Status.GuaranteedInstanceManagerCPU[longhorn.InstanceManagerTypeAllInOne] = guaranteedInstanceManagerCPU.Value
		}

		// Node CPU
		node, err := c.ds.GetKubernetesNode(sample.Spec.AttachToNodeID)
		if err != nil {
			return err
		}
		sample.Status.OwnerAllocatableCPU = node.Status.Allocatable.Cpu().MilliValue()

		// Node Memory
		sample.Status.OwnerAllocatableMemory = node.Status.Allocatable.Memory().Value() / 1024 / 1024

		_, err = c.ds.GetJob(getSampleName(sample.Name))
		if err == nil {
			return nil
		}

		if !apierrors.IsNotFound(err) {
			return err
		}

		_, err = c.createJob(sample)
		if err != nil {
			c.updateRecord(record,
				sampleRecordTypeError, longhorn.SampleStateError,
				fmt.Sprintf(constant.EventReasonFailedCreatingFmt, types.KubernetesKindJob, "for "+sample.Name),
				err.Error(),
			)
			return nil
		}

		c.updateRecord(record,
			sampleRecordTypeNormal, longhorn.SampleStateStarted,
			"", "",
		)

	case longhorn.SampleStateDeleting:
		err := c.cleanup(sample)
		if err != nil {
			return err
		}
		return c.ds.RemoveFinalizerForSample(sample)
	case longhorn.SampleStateStarted:
		log.Info("Sampling")
	case longhorn.SampleStateComplete:
		log.Info("Completed sampling")
	}

	return nil
}

func (c *SampleController) handleStatusUpdate(record *sampleRecord, sample *longhorn.Sample, existingSample *longhorn.Sample, err error, log logrus.FieldLogger) {
	switch record.recordType {
	case sampleRecordTypeError:
		sample.Status.Conditions = types.SetCondition(
			sample.Status.Conditions,
			longhorn.SampleConditionTypeError,
			longhorn.ConditionStatusTrue,
			record.reason,
			record.message,
		)
		fallthrough
	case sampleRecordTypeNormal:
		sample.Status.State = record.nextState
	default:
		return
	}

	if !reflect.DeepEqual(existingSample.Status, sample.Status) {
		if _, err = c.ds.UpdateSampleStatus(sample); err != nil {
			log.WithError(err).Debugf("Requeue %v due to error", sample.Name)
			c.enqueue(sample)
			err = nil
			return
		}
	}

	if sample.Status.State != existingSample.Status.State {
		log.Infof(SampleMsgRequeueNextPhaseFmt, sample.Name, sample.Status.State)
	}
}

func (c *SampleController) cleanup(sample *longhorn.Sample) (err error) {
	log := getLoggerForSample(c.logger, sample)

	defer func() {
		if err == nil {
			return
		}

		log.WithError(err).Error("Failed to delete Sample")
		sample.Status.Conditions = types.SetCondition(sample.Status.Conditions,
			string(longhorn.SampleConditionTypeError), longhorn.ConditionStatusTrue, "", err.Error())
	}()

	volumes, err := c.ds.ListVolumes()
	if err != nil {
		return err
	}

	for _, volume := range volumes {
		if volume.Spec.LastAttachedBy != sample.Name {
			continue
		}

		err := c.ds.DeleteVolume(volume.Name)
		if err != nil {
			return err
		}
	}

	sampleName := getSampleName(sample.Name)
	_, err = c.ds.GetJob(sampleName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return
	}

	log.WithField("job", sampleName).Debug("Deleting job")
	return c.ds.DeleteJob(sampleName)
}

func (c *SampleController) isResponsibleFor(Sample *longhorn.Sample) bool {
	return isControllerResponsibleFor(c.controllerID, c.ds, Sample.Name, "", Sample.Status.OwnerID)
}

func (c *SampleController) updateRecord(record *sampleRecord, recordType sampleRecordType, nextState longhorn.SampleState, reason, message string) {
	record.recordType = recordType
	record.nextState = nextState
	record.reason = reason
	record.message = message
}

func (c *SampleController) createJob(sample *longhorn.Sample) (*batchv1.Job, error) {
	serviceAccountName, err := c.ds.GetLonghornServiceAccountName()
	if err != nil {
		return nil, err
	}

	return c.ds.CreateJob(c.newJob(sample, serviceAccountName))
}

func (c *SampleController) newJob(sample *longhorn.Sample, serviceAccount string) *batchv1.Job {
	cmd := []string{
		"longhorn-manager", "sample", sample.Name,
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:            getSampleName(sample.Name),
			Namespace:       c.namespace,
			OwnerReferences: datastore.GetOwnerReferencesForSample(sample),
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: pointer.Int32Ptr(SampleBackoffLimit),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: sample.Name,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: serviceAccount,
					Containers: []corev1.Container{
						{
							Name:    getSampleName(sample.Name),
							Image:   c.managerImage,
							Command: cmd,
							Env: []corev1.EnvVar{
								{
									Name: "POD_NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
								{
									Name:  "NODE_NAME",
									Value: c.controllerID,
								},
							},
							// VolumeMounts: []corev1.VolumeMount{
							// 	{
							// 		Name:      "engine",
							// 		MountPath: types.EngineBinaryDirectoryOnHost,
							// 	},
							// },
						},
					},
					// Volumes: []corev1.Volume{
					// 	{
					// 		Name: "engine",
					// 		VolumeSource: corev1.VolumeSource{
					// 			HostPath: &corev1.HostPathVolumeSource{
					// 				Path: types.EngineBinaryDirectoryOnHost,
					// 			},
					// 		},
					// 	},
					// },
					RestartPolicy: corev1.RestartPolicyOnFailure,
					NodeSelector: map[string]string{
						corev1.LabelHostname: c.controllerID,
					},
				},
			},
		},
	}

	return job
}

func getSampleName(sampleName string) string {
	return SampleNamePrefix + sampleName
}
