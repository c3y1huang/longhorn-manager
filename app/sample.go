package app

import (
	"context"
	"fmt"
	"math"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"k8s.io/client-go/rest"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const (
	Mi = (1024 * 1024)

	SampleVolumeAttachTimeout = 60  // 1 minutes
	SamplePodRunningTimeout   = 120 // 2 minutes

	SampleExtendDuration = 10 * time.Minute // 10 minutes
	SampleTimeout        = 5 * time.Minute
)

func SampleCmd() cli.Command {
	return cli.Command{
		Name:  "sample",
		Flags: []cli.Flag{
			// cli.StringFlag{
			// 	Name:  FlagKubeConfig,
			// 	Usage: "Specify path to kube config (optional)",
			// },
			// cli.StringFlag{
			// 	Name:   FlagNamespace,
			// 	EnvVar: types.EnvPodNamespace,
			// },
			// cli.StringFlag{
			// 	Name:  FlagManagerURL,
			// 	Usage: "Longhorn manager API URL",
			// },
		},
		Action: func(c *cli.Context) {
			if err := sample(c); err != nil {
				logrus.Fatalln(err)
			}
		},
	}
}

func sample(cli *cli.Context) error {
	logger := logrus.StandardLogger()

	if cli.NArg() != 1 {
		return errors.New("Sample name is required")
	}
	jobName := cli.Args()[0]

	c, err := NewSampleContext(jobName, logger)
	if err != nil {
		return errors.Wrap(err, "failed to initialize")
	}

	c.collectInitialUsage, err = c.getUsageFromPodMetrics(c.collectInitialUsage)
	if err != nil {
		return err
	}

	volumeAPI := c.api.Volume

	defer c.cleanupVolumes()
	err = c.createVolumes(volumeAPI)
	if err != nil {
		return errors.Wrap(err, "failed to create Volumes")
	}
	err = c.createPersistentVolumes(volumeAPI)
	if err != nil {
		return errors.Wrap(err, "failed to create PVs")
	}
	err = c.createPersistentVolumeClaims(volumeAPI)
	if err != nil {
		return errors.Wrap(err, "failed to create PVCs")
	}

	defer c.cleanupPods()
	err = c.createPods(volumeAPI)
	if err != nil {
		return errors.Wrap(err, "failed to create Pods")
	}

	c.syncNumberOfWorklodsCreated()

	defer func() {
		sample, err := c.getSample()
		if err != nil {
			c.logger.WithError(err).Error()
			return
		}
		newStatus := sample.Status.DeepCopy()
		newStatus.NumberOfWorkloadsCreated = c.collectNumberOfWorkloadsCreated
		newStatus.AverageUsage = c.getStatusAverageUsage()

		// estimateMaxWorkloads := []int64{}

		// for _, instanceMangerType := range []longhorn.InstanceManagerType{longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerTypeReplica} {
		// 	statusGuaranteedCPU := c.customResource.Status.GuaranteedEngineManagerCPU
		// 	if instanceMangerType == longhorn.InstanceManagerTypeReplica {
		// 		statusGuaranteedCPU = c.customResource.Status.GuaranteedReplicaManagerCPU
		// 	}
		// 	guarenteedCPU, err := strconv.ParseInt(statusGuaranteedCPU, 10, 64)
		// 	if err != nil {
		// 		c.logger.WithError(err).Error()
		// 		return
		// 	}

		// 	switch c.averageCPUOfInstances[instanceMangerType] {
		// 	case 0:
		// 		estimateMaxWorkloads = append(estimateMaxWorkloads, sample.Status.OwnerAllocatableCPU*(guarenteedCPU/100))
		// 		c.logger.Infof("[DEBUG][0] %v", estimateMaxWorkloads)
		// 	default:
		// 		guarenteedCPU := sample.Status.OwnerAllocatableCPU * guarenteedCPU / 100
		// 		estimateMaxWorkloads = append(estimateMaxWorkloads, int64(float64(guarenteedCPU)/c.averageCPUOfInstances[instanceMangerType]))
		// 		c.logger.Infof("[DEBUG][1] %v", estimateMaxWorkloads)
		// 	}
		// }

		// switch c.averageMemoryOfAllInstance {
		// case 0.0:
		// 	estimateMaxWorkloads = append(estimateMaxWorkloads, sample.Status.OwnerAllocatableMemory)
		// 	c.logger.Infof("[DEBUG][2] %v", estimateMaxWorkloads)
		// default:
		// 	estimateMaxWorkloads = append(estimateMaxWorkloads, sample.Status.OwnerAllocatableMemory/int64(c.averageMemoryOfAllInstance))
		// 	c.logger.Infof("[DEBUG][3] %v", estimateMaxWorkloads)
		// }
		// sort.Slice(estimateMaxWorkloads, func(i, j int) bool {
		// 	return estimateMaxWorkloads[i] < estimateMaxWorkloads[j]
		// })

		// newStatus.EstimateMaximumWorkloads = int(estimateMaxWorkloads[0])
		// // for _, estimateMaxWorkload := range estimateMaxWorkloads {
		// // 	if newStatus.EstimateMaximumWorkloads != 0 {
		// // 		break
		// // 	}
		// // 	if estimateMaxWorkload == 0 {
		// // 		continue
		// // 	}
		// // 	newStatus.EstimateMaximumWorkloads = int(estimateMaxWorkload)
		// // }

		newStatus.State = longhorn.SampleStateComplete
		err = c.updateStatus(*newStatus)
		if err != nil {
			c.logger.WithError(err).Error("Failed to update Sample status")
		}
	}()

	if c.customResource.Spec.DurationInMinutes == 0 {
		c.logger.Infof("Sampling creating workloads")
		collectStart := time.Now()
		collectDuration := time.Duration(time.Minute)
		for time.Since(collectStart) <= collectDuration {
			c.collectUsage, err = c.getUsageFromPodMetrics(c.collectUsage)
			if err != nil {
				return err
			}

			time.Sleep(time.Second)
			continue
		}
		return nil
	}

	time.Sleep(5 * time.Minute)

	c.logger.Infof("Sampling running workloads")

	// move away from resource used for creation
	c.collectUsage = map[longhorn.InstanceManagerType]SampleUsages{}

	collectStart := time.Now()
	collectDuration := time.Duration(c.customResource.Spec.DurationInMinutes) * time.Minute
	for time.Since(collectStart) <= collectDuration {
		c.collectUsage, err = c.getUsageFromPodMetrics(c.collectUsage)
		if err != nil {
			return err
		}

		time.Sleep(time.Second)
		c.logger.Debug("Running overtime")
		continue
	}
	return nil
}

type SampleWorkload struct {
	All SampleResourceUsage
	One SampleResourceUsage
}

type SampleResourceUsage struct {
	CPUCores    float64
	MemoryBytes int
}

func (c *SampleContext) getStatusAverageUsage() map[longhorn.InstanceManagerType]longhorn.SampleWorkload {
	usage := map[longhorn.InstanceManagerType]SampleWorkload{}
	for _, instanceMangerType := range []longhorn.InstanceManagerType{longhorn.InstanceManagerTypeEngine, longhorn.InstanceManagerTypeReplica, longhorn.InstanceManagerTypeAllInOne} {
		initialCPU := averageFloat64s(c.collectInitialUsage[instanceMangerType].CPUCores)
		initialMemory := averageInts(c.collectInitialUsage[instanceMangerType].MemoryMiBs)

		logrus.Infof("Calculating average CPU: %v - %v", averageFloat64s(c.collectUsage[instanceMangerType].CPUCores), initialCPU)
		logrus.Infof("Calculating average Memory: %v - %v", averageInts(c.collectUsage[instanceMangerType].MemoryMiBs), initialMemory)

		averageCPUOfInstance := math.Max(0, averageFloat64s(c.collectUsage[instanceMangerType].CPUCores)-initialCPU)
		averageCPUOfOne := averageCPUOfInstance / float64(c.collectNumberOfWorkloadsCreated)
		c.averageCPUOfInstances[instanceMangerType] = c.averageCPUOfInstances[instanceMangerType] + averageCPUOfOne

		averageMemoryOfInstance := max(0, averageInts(c.collectUsage[instanceMangerType].MemoryMiBs)-initialMemory)
		averageMemoryOfOne := averageMemoryOfInstance / c.collectNumberOfWorkloadsCreated
		c.averageMemoryOfAllInstance = c.averageMemoryOfAllInstance + averageMemoryOfOne

		// usage[instanceMangerType] = longhorn.SampleWorkload{
		// 	All: longhorn.SampleResourceUsage{
		// 		CPUCores:    fmt.Sprintf("%.2fm", averageCPUOfInstance),
		// 		MemoryBytes: fmt.Sprintf("%dMi", averageMemoryOfInstance),
		// 	},
		// 	One: longhorn.SampleResourceUsage{
		// 		CPUCores:    fmt.Sprintf("%.2fm", averageCPUOfOne),
		// 		MemoryBytes: fmt.Sprintf("%dMi", averageMemoryOfOne),
		// 	},
		// }
		usage[instanceMangerType] = SampleWorkload{
			All: SampleResourceUsage{
				CPUCores:    averageCPUOfInstance,
				MemoryBytes: averageMemoryOfInstance,
			},
			One: SampleResourceUsage{
				CPUCores:    averageCPUOfOne,
				MemoryBytes: averageMemoryOfOne,
			},
		}
	}

	engineUsage := usage[longhorn.InstanceManagerTypeEngine]
	replicaUsage := usage[longhorn.InstanceManagerTypeReplica]
	combineUsage := usage[longhorn.InstanceManagerTypeAllInOne]
	// if _, ok := usage[longhorn.InstanceManagerTypeAllInOne]; !ok {
	// engineUsage := usage[longhorn.InstanceManagerTypeEngine]
	// replicaUsage := usage[longhorn.InstanceManagerTypeReplica]
	// combineUsage := usage[longhorn.InstanceManagerTypeAllInOne]
	usage[longhorn.InstanceManagerTypeAllInOne] = SampleWorkload{
		All: SampleResourceUsage{
			CPUCores:    combineUsage.All.CPUCores + engineUsage.All.CPUCores + replicaUsage.All.CPUCores,
			MemoryBytes: combineUsage.All.MemoryBytes + engineUsage.All.MemoryBytes + replicaUsage.All.MemoryBytes,
		},
		One: SampleResourceUsage{
			CPUCores:    combineUsage.One.CPUCores + engineUsage.One.CPUCores + replicaUsage.One.CPUCores,
			MemoryBytes: combineUsage.One.MemoryBytes + engineUsage.One.MemoryBytes + replicaUsage.One.MemoryBytes,
		},
	}
	combineUsage = usage[longhorn.InstanceManagerTypeAllInOne]
	// }
	// combineUsage := map[longhorn.InstanceManagerType]longhorn.SampleWorkload{
	// 	longhorn.InstanceManagerTypeAllInOne: longhorn.SampleWorkload{
	// 		All: longhorn.SampleResourceUsage{
	// 			CPUCores:    fmt.Sprintf("%.2fm", engineUsage.All.CPUCores + replicaUsage.All.CPUCores),
	// 			MemoryBytes: fmt.Sprintf("%dMi", engineUsage.All.MemoryBytes + replicaUsage.All.MemoryBytes),
	// 		},
	// 		One: longhorn.SampleResourceUsage{
	// 			CPUCores:    fmt.Sprintf("%.2fm", engineUsage.One.CPUCores + replicaUsage.One.CPUCores),
	// 			MemoryBytes: fmt.Sprintf("%dMi", engineUsage.One.MemoryBytes + replicaUsage.One.MemoryBytes),
	// 		},
	// 	},
	// }
	// combineUsage[longhorn.InstanceManagerTypeAllInOne] = longhorn.SampleWorkload{
	// 	All: longhorn.SampleResourceUsage{
	// 		CPUCores:    fmt.Sprintf("%.2fm", engineUsage.All.CPUCores + replicaUsage.All.CPUCores),
	// 		MemoryBytes: fmt.Sprintf("%dMi", engineUsage.All.MemoryBytes + replicaUsage.All.MemoryBytes),
	// 	},
	// 	One: longhorn.SampleResourceUsage{
	// 		CPUCores:    fmt.Sprintf("%.2fm", engineUsage.One.CPUCores + replicaUsage.One.CPUCores),
	// 		MemoryBytes: fmt.Sprintf("%dMi", engineUsage.One.MemoryBytes + replicaUsage.One.MemoryBytes),
	// 	},
	// }

	return map[longhorn.InstanceManagerType]longhorn.SampleWorkload{
		longhorn.InstanceManagerTypeAllInOne: {
			All: longhorn.SampleResourceUsage{
				CPUCores:    fmt.Sprintf("%.2fm", combineUsage.All.CPUCores),
				MemoryBytes: fmt.Sprintf("%dMi", combineUsage.All.MemoryBytes),
			},
			One: longhorn.SampleResourceUsage{
				CPUCores:    fmt.Sprintf("%.2fm", combineUsage.One.CPUCores),
				MemoryBytes: fmt.Sprintf("%dMi", combineUsage.One.MemoryBytes),
			},
		},
		longhorn.InstanceManagerTypeEngine: {
			All: longhorn.SampleResourceUsage{
				CPUCores:    fmt.Sprintf("%.2fm", engineUsage.All.CPUCores),
				MemoryBytes: fmt.Sprintf("%dMi", engineUsage.All.MemoryBytes),
			},
			One: longhorn.SampleResourceUsage{
				CPUCores:    fmt.Sprintf("%.2fm", engineUsage.One.CPUCores),
				MemoryBytes: fmt.Sprintf("%dMi", engineUsage.One.MemoryBytes),
			},
		},
		longhorn.InstanceManagerTypeReplica: {
			All: longhorn.SampleResourceUsage{
				CPUCores:    fmt.Sprintf("%.2fm", replicaUsage.All.CPUCores),
				MemoryBytes: fmt.Sprintf("%dMi", replicaUsage.All.MemoryBytes),
			},
			One: longhorn.SampleResourceUsage{
				CPUCores:    fmt.Sprintf("%.2fm", replicaUsage.One.CPUCores),
				MemoryBytes: fmt.Sprintf("%dMi", replicaUsage.One.MemoryBytes),
			},
		},
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func averageFloat64s(numbers []float64) float64 {
	if len(numbers) == 0 {
		return 0.0
	}

	total := 0.0
	for _, number := range numbers {
		total += number
	}

	return total / float64(len(numbers))
}

func averageInts(numbers []int) int {
	if len(numbers) == 0 {
		return 0.0
	}

	total := 0
	for _, number := range numbers {
		total += number
	}

	return total / len(numbers)
}

type VolumeName string
type SampleContext struct {
	logger logrus.FieldLogger

	namespace string
	// nodeName  string

	kubeClient    clientset.Interface
	lhClient      lhclientset.Interface
	metricsClient metricsclientset.Interface

	api *longhornclient.RancherClient

	customResource       *longhorn.Sample
	podForReplication    *corev1.Pod
	volumeForReplication *longhorn.Volume

	workloads    map[VolumeName]*Workload
	errorMessage string

	collectNumberOfWorkloadsCreated int
	collectInitialUsage             map[longhorn.InstanceManagerType]SampleUsages
	collectUsage                    map[longhorn.InstanceManagerType]SampleUsages

	averageCPUOfInstances      map[longhorn.InstanceManagerType]float64
	averageMemoryOfAllInstance int
}

type SampleUsages struct {
	CPUCores   []float64
	MemoryMiBs []int
}

type Workload struct {
	namespace string

	podName                      string
	persistentVolumeCreated      bool
	persistentVolumeClaimCreated bool
}

func NewSampleContext(name string, logger logrus.FieldLogger) (*SampleContext, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		return nil, fmt.Errorf("cannot detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get Kubernetes clientset")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get Longhorn clientset")
	}

	metricsClient, err := metricsclientset.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get Metrics clientset")
	}

	clientOpts := &longhornclient.ClientOpts{
		Url:     types.GetDefaultManagerURL(),
		Timeout: HTTPClientTimout,
	}
	apiClient, err := longhornclient.NewRancherClient(clientOpts)
	if err != nil {
		return nil, errors.Wrapf(err, "could not create longhorn-manager api client")
	}

	sample, err := lhClient.LonghornV1beta2().Samples(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get Sample %v", name)
	}

	pod, err := kubeClient.CoreV1().Pods("default").Get(context.TODO(), sample.Spec.PodForReplication, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get Pod %v", sample.Spec.PodForReplication)
	}
	if len(pod.Spec.Volumes) < 1 || pod.Spec.Volumes[0].PersistentVolumeClaim == nil {
		return nil, fmt.Errorf("invalid Pod spec volumes %+v", pod.Spec.Volumes)
	}

	pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(context.TODO(), pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get PersistentVolumeClaim %v", pod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName)
	}

	volume, err := lhClient.LonghornV1beta2().Volumes(namespace).Get(context.TODO(), pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get Volume %v", pvc.Spec.VolumeName)
	}

	logger = logger.WithFields(logrus.Fields{
		"namespace":         namespace,
		"sample":            sample.Name,
		"concurrency":       sample.Spec.Concurrency,
		"durationInMinutes": sample.Spec.DurationInMinutes,
	})

	return &SampleContext{
		logger:                logger,
		namespace:             namespace,
		kubeClient:            kubeClient,
		lhClient:              lhClient,
		metricsClient:         metricsClient,
		api:                   apiClient,
		customResource:        sample,
		podForReplication:     pod,
		volumeForReplication:  volume,
		workloads:             map[VolumeName]*Workload{},
		collectUsage:          map[longhorn.InstanceManagerType]SampleUsages{},
		collectInitialUsage:   map[longhorn.InstanceManagerType]SampleUsages{},
		averageCPUOfInstances: map[longhorn.InstanceManagerType]float64{},
	}, nil
}

func (c *SampleContext) resetErrorMessage() {
	c.errorMessage = ""
}

func (c *SampleContext) syncNumberOfWorklodsCreated() {
	numberOfWorklodsCreated := 0
	for _, workload := range c.workloads {
		if workload.podName == "" {
			continue
		}
		numberOfWorklodsCreated += 1
	}

	c.collectNumberOfWorkloadsCreated = numberOfWorklodsCreated
}

func (c *SampleContext) getUsageFromPodMetrics(usage map[longhorn.InstanceManagerType]SampleUsages) (map[longhorn.InstanceManagerType]SampleUsages, error) {
	podMetrics, err := c.listPodMetrics()
	if err != nil {
		return usage, err
	}
	for _, podMetric := range podMetrics.Items {
		instanceManagerName := podMetric.GetName()
		instanceManagerType := getInstanceManagerTypeFromName(instanceManagerName)
		if _, ok := usage[instanceManagerType]; !ok {
			usage[instanceManagerType] = SampleUsages{}
		}

		var usageCPUCores float64
		var usageMemoryMiB int64
		for _, c := range podMetric.Containers {
			usageCPUCores += float64(c.Usage.Cpu().MilliValue())
			usageMemoryByte := c.Usage.Memory().Value()
			usageMemoryMiB = usageMemoryByte / 1024 / 1024 // convert memory usage from bytes to MiB
		}

		usage[instanceManagerType] = SampleUsages{
			CPUCores:   append(usage[instanceManagerType].CPUCores, usageCPUCores),
			MemoryMiBs: append(usage[instanceManagerType].MemoryMiBs, int(usageMemoryMiB)),
		}
	}
	return usage, nil
}

func (c *SampleContext) listPodMetrics() (*metricsv1beta1.PodMetricsList, error) {
	return c.metricsClient.MetricsV1beta1().PodMetricses(c.namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: getInstanceManagerLabelSelector(c.customResource.Spec.AttachToNodeID),
	})
}

func getInstanceManagerLabelSelector(nodeID string) string {
	componentLabel := types.GetLonghornLabelComponentKey() + "=" + types.LonghornLabelInstanceManager
	nodeLabel := types.GetLonghornLabelKey(types.LonghornLabelNode) + "=" + nodeID
	return componentLabel + "," + nodeLabel
}

func getInstanceManagerTypeFromName(imName string) longhorn.InstanceManagerType {
	switch {
	case strings.Contains(imName, types.GetInstanceManagerPrefix(longhorn.InstanceManagerTypeEngine)):
		return longhorn.InstanceManagerTypeEngine
	case strings.Contains(imName, types.GetInstanceManagerPrefix(longhorn.InstanceManagerTypeReplica)):
		return longhorn.InstanceManagerTypeReplica
	case strings.Contains(imName, types.GetInstanceManagerPrefix(longhorn.InstanceManagerTypeAllInOne)):
		return longhorn.InstanceManagerTypeAllInOne
	default:
		return ""
	}
}

func (c *SampleContext) cleanupVolumes() {
	for volumeName, _ := range c.workloads {
		err := c.lhClient.LonghornV1beta2().Volumes(c.namespace).Delete(context.TODO(), string(volumeName), metav1.DeleteOptions{})
		if err != nil {
			c.logger.WithError(err).Warn("Cannot Delete Pod for Volume")
		}
	}

	for volumeName, _ := range c.workloads {
	recheck:
		_, err := c.lhClient.LonghornV1beta2().Volumes(c.namespace).Get(context.TODO(), string(volumeName), metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
		}
		start := time.Now()
		for time.Since(start) < SampleTimeout {
			goto recheck
		}
		c.logger.Errorf("Timeout deleting Volume %v", volumeName)
	}

}

func (c *SampleContext) createVolumes(volumeAPI longhornclient.VolumeOperations) (err error) {
	concurrentLimiter := make(chan struct{}, c.customResource.Spec.Concurrency)
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		c.resetErrorMessage()

	}()
	for i := 0; i < c.customResource.Spec.NumberToReplicate; i++ {
		volumeName := fmt.Sprintf("%v-%v", c.customResource.Name, i)
		log := c.logger.WithField("Volume", volumeName)
		if c.errorMessage != "" {
			log.Warn(c.errorMessage)
			break
		}

		wg.Add(1)
		go func(volumeName string) {
			concurrentLimiter <- struct{}{}
			defer func() {
				<-concurrentLimiter
				wg.Done()
			}()

			err := c.createVolume(volumeName, volumeAPI, log)
			if err != nil {
				log.WithError(err).Warn("Cannot create more Volumes")
				return
			}
		}(volumeName)
	}
	return nil
}

func (c *SampleContext) createVolume(name string, volumeAPI longhornclient.VolumeOperations, log logrus.FieldLogger) error {
	if _, exist := c.workloads[VolumeName(name)]; exist {
		return nil
	}

	readyAndSchedulableNodes, err := c.listReadyAndSchedulableNodes()
	if err != nil {
		return errors.Wrapf(err, "failed to get ready and schedulable Nodes")
	}

	newVolume := &longhornclient.Volume{
		Name:             name,
		Size:             fmt.Sprint(c.volumeForReplication.Spec.Size),
		NumberOfReplicas: int64(len(readyAndSchedulableNodes)),
	}
	log.Infof("Creating Volume")
	volume, err := volumeAPI.Create(newVolume)
	if err != nil && !types.ErrorAlreadyExists(err) {
		return err
	}

	createdTime := time.Now()
	defer func() {
		if err != nil {
			c.errorMessage = errors.Wrapf(err, "Failed to attach Volume %v", name).Error()
		}
	}()
	for {
		switch longhorn.VolumeState(volume.State) {
		case longhorn.VolumeStateAttached:
			c.workloads[VolumeName(name)] = &Workload{}
		case longhorn.VolumeStateDetached:
			// Automatically attach the volume
			// Disable the volume's frontend make sure that pod cannot use the volume during the recurring job.
			// This is necessary so that we can safely detach the volume when finishing the job.
			log.Infof("Attaching Volume to node %v", c.customResource.Spec.AttachToNodeID)
			if _, err = volumeAPI.ActionAttach(volume, &longhornclient.AttachInput{
				HostId:     c.customResource.Spec.AttachToNodeID,
				AttachedBy: c.customResource.Name,
			}); err != nil {
				return err
			}

			_, err := waitForVolumeState(newVolume.Name, string(longhorn.VolumeStateAttached), SampleVolumeAttachTimeout, c.api)
			if err != nil {
				return err
			}
			fallthrough
		default:
			if time.Since(createdTime) < SampleVolumeAttachTimeout*time.Second {
				time.Sleep(time.Second)
				log.Infof("Wait for volume %v to attach %v", name, volume.State)
				volume, err = volumeAPI.ById(name)
				if err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("timeout")
		}
		break
	}
	return nil
}

func (c *SampleContext) createPersistentVolumes(volumeAPI longhornclient.VolumeOperations) (err error) {
	concurrentLimiter := make(chan struct{}, c.customResource.Spec.Concurrency)
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		c.resetErrorMessage()

	}()
	for volumeName := range c.workloads {
		log := c.logger.WithField("Volume", volumeName)
		if c.errorMessage != "" {
			log.Warn(c.errorMessage)
			break
		}

		volume, err := volumeAPI.ById(string(volumeName))
		if err != nil {
			return err
		}

		wg.Add(1)
		go func(volumeName VolumeName) {
			concurrentLimiter <- struct{}{}
			defer func() {
				<-concurrentLimiter
				wg.Done()
			}()

			log.Info("Creating PersistenVolume for Volume")
			_, err = volumeAPI.ActionPvCreate(volume, &longhornclient.PVCreateInput{
				FsType: "ext4",
				PvName: string(volumeName),
			})
			if err != nil {
				log.WithError(err).Warn("Cannot PersistenVolume for Volume")
				return
			}
			c.workloads[volumeName].persistentVolumeCreated = true
		}(volumeName)
	}
	return nil
}

func (c *SampleContext) createPersistentVolumeClaims(volumeAPI longhornclient.VolumeOperations) (err error) {
	concurrentLimiter := make(chan struct{}, c.customResource.Spec.Concurrency)
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		c.resetErrorMessage()

	}()
	for volumeName, workload := range c.workloads {
		log := c.logger.WithField("Volume", volumeName)
		if c.errorMessage != "" {
			log.Warn(c.errorMessage)
			break
		}

		if !workload.persistentVolumeCreated {
			continue
		}

		volume, err := volumeAPI.ById(string(volumeName))
		if err != nil {
			return err
		}

		wg.Add(1)
		go func(volumeName VolumeName) {
			concurrentLimiter <- struct{}{}
			defer func() {
				<-concurrentLimiter
				wg.Done()
			}()

			log.Info("Creating PersistenVolumeClaim for Volume")
			_, err = volumeAPI.ActionPvcCreate(volume, &longhornclient.PVCCreateInput{
				Namespace: c.podForReplication.Namespace,
				PvcName:   string(volumeName),
			})
			if err != nil {
				log.WithError(err).Warn("Cannot PersistenVolume for Volume")
				return
			}
			c.workloads[volumeName].persistentVolumeClaimCreated = true
			c.workloads[volumeName].namespace = c.podForReplication.Namespace
		}(volumeName)
	}
	return nil
}

func (c *SampleContext) createPods(volumeAPI longhornclient.VolumeOperations) (err error) {
	concurrentLimiter := make(chan struct{}, c.customResource.Spec.Concurrency)
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		c.resetErrorMessage()

	}()
	for volumeName, workload := range c.workloads {
		log := c.logger.WithField("Volume", volumeName)
		if c.errorMessage != "" {
			log.Warn(c.errorMessage)
			break
		}

		if !workload.persistentVolumeClaimCreated {
			continue
		}

		wg.Add(1)
		go func(volumeName VolumeName) {
			concurrentLimiter <- struct{}{}
			defer func() {
				<-concurrentLimiter
				wg.Done()
			}()

			newPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("%s-%s", c.podForReplication.Name, volumeName),
					Namespace: c.podForReplication.Namespace,
				},
				Spec: *c.podForReplication.Spec.DeepCopy(),
			}

			newPod.Spec.Volumes[0].PersistentVolumeClaim.ClaimName = string(volumeName)
			newPod.Spec.NodeName = c.customResource.Spec.AttachToNodeID

			log.Infof("Creating duplicated Pod %v", newPod.Name)
			pod, err := c.kubeClient.CoreV1().Pods(newPod.Namespace).Create(context.TODO(), newPod, metav1.CreateOptions{})
			if err != nil {
				log.WithError(err).Warn("Cannot Create Pod for Volume")
				return
			}

			c.workloads[volumeName].podName = pod.Name
			_, err = waitForPodPhase(pod.Name, pod.Namespace, corev1.PodRunning, SamplePodRunningTimeout, c.kubeClient)
			if err != nil {
				log.WithError(err).Warn("Failed to run Pod for Volume")
				return
			}
		}(volumeName)
	}
	return nil
}

func (c *SampleContext) cleanupPods() {
	for _, workload := range c.workloads {
		if workload.podName == "" {
			continue
		}
		err := c.kubeClient.CoreV1().Pods(workload.namespace).Delete(context.TODO(), workload.podName, metav1.DeleteOptions{})
		if err != nil {
			c.logger.WithError(err).Warn("Cannot Delete Pod for Volume")
		}
	}

	for _, workload := range c.workloads {
	recheck:
		_, err := c.kubeClient.CoreV1().Pods(workload.namespace).Get(context.TODO(), workload.podName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
		}
		start := time.Now()
		for time.Since(start) < SampleTimeout {
			goto recheck
		}
		c.logger.Errorf("Timeout deleting Pod %v", workload.podName)
	}

}

func (c *SampleContext) listReadyAndSchedulableNodes() (map[string]*longhorn.Node, error) {
	nodeList, err := c.lhClient.LonghornV1beta2().Nodes(c.namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get Nodes")
	}

	itemMap := make(map[string]*longhorn.Node)
	for _, node := range nodeList.Items {
		// Cannot use cached object from lister
		itemMap[node.Name] = node.DeepCopy()
	}
	return datastore.FilterSchedulableNodes(datastore.FilterReadyNodes(itemMap)), nil
}

func (c *SampleContext) getSample() (*longhorn.Sample, error) {
	return c.lhClient.LonghornV1beta2().Samples(c.namespace).Get(context.TODO(), c.customResource.Name, metav1.GetOptions{})
}

func (c *SampleContext) updateStatus(status longhorn.SampleStatus) error {
	sample, err := c.lhClient.LonghornV1beta2().Samples(c.namespace).Get(context.TODO(), c.customResource.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if reflect.DeepEqual(sample.Status, status) {
		return nil
	}
	sample.Status = status

	sample, err = c.lhClient.LonghornV1beta2().Samples(c.namespace).UpdateStatus(context.TODO(), sample, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	c.customResource = sample
	return nil
}
