package app

import (
	"context"
	// "encoding/json"
	"fmt"
	// "math/rand"
	// "os"
	// "sort"
	// "strconv"
	// "strings"
	// "sync"
	"time"

	// "github.com/pkg/errors"
	// "github.com/sirupsen/logrus"
	// "github.com/urfave/cli"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	// "k8s.io/client-go/rest"
	clientset "k8s.io/client-go/kubernetes"

	// etypes "github.com/longhorn/longhorn-engine/pkg/types"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	// longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	// lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	// "github.com/longhorn/longhorn-manager/types"
	// "github.com/longhorn/longhorn-manager/util"
)

// waitForVolumeState timeout in second
func waitForVolumeState(volumeName, state string, timeout int, api *longhornclient.RancherClient) (*longhornclient.Volume, error) {
	volumeAPI := api.Volume
	for i := 0; i < timeout; i++ {
		volume, err := volumeAPI.ById(volumeName)
		if err == nil {
			if volume.State == state {
				return volume, nil
			}
		}
		time.Sleep(1 * time.Second)
	}

	return nil, fmt.Errorf("timeout waiting for volume %v to be in state %v", volumeName, state)
}

func waitForPodPhase(name, namespace string, phase corev1.PodPhase, timeout int, kubeClient clientset.Interface) (*corev1.Pod, error) {
	for i := 0; i < timeout; i++ {
		pod, err := kubeClient.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if pod.Status.Phase == phase {
			return pod, nil
		}
		time.Sleep(1 * time.Second)
	}
	return nil, fmt.Errorf("timeout waiting for Pod %v to be in phase %v", name, phase)
}
