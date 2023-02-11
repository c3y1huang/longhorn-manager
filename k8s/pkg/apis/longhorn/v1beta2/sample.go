package v1beta2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type SampleState string

const (
	SampleStateComplete    = SampleState("Complete")
	SampleStateDeleting    = SampleState("Deleting")
	SampleStateError       = SampleState("Error")
	SampleStateInitialized = SampleState("Initialized")
	SampleStateNone        = SampleState("")
	SampleStateStarted     = SampleState("Sampling")
)

const (
	SampleConditionTypeError = "Error"
)

// SampleSpec defines the desired state of the Longhorn stress job
type SampleSpec struct {
	// The sample Job name.
	// +optional
	Name string `json:"name"`
	// A running Pod name to use for sampling.
	// +optional
	PodForReplication string `json:"podForReplication"`
	// The number of Pod to replicate.
	// +optional
	NumberToReplicate int `json:"numberToReplicate"`
	// The node ID to attach the volume.
	// +optional
	AttachToNodeID string `json:"attachToNodeID"`
	// The concurrency of volume attachment.
	// +optional
	Concurrency int `json:"concurrency"`
	// Duration in minutes.
	// +optional
	DurationInMinutes int `json:"durationInMinutes"`
}

// SampleStatus defines the observed state of the Longhorn stress job
type SampleStatus struct {
	// The owner ID of this custom resource.
	// +optional
	OwnerID string `json:"ownerID"`
	// Record the owner allocatable CPU.
	// +optional
	OwnerAllocatableCPU int64 `json:"ownerAllocatableCPU"`
	// Record the owner allocatable memory.
	// +optional
	OwnerAllocatableMemory int64 `json:"ownerAllocatableMemory"`

	// Record the guaranteed instance manager CPU setting value.
	// +optional
	GuaranteedInstanceManagerCPU map[InstanceManagerType]string `json:"guaranteedInstanceManagerCPU"`

	// The sampling state.
	// +optional
	State SampleState `json:"state"`
	// +optional
	// +nullable
	Conditions []Condition `json:"conditions"`

	// The number of workload of volume/pv/pvc/pod created.
	// +optional
	NumberOfWorkloadsCreated int `json:"numberOfWorkloadsCreated"`
	// The average resource usage for engine/replica instance managers.
	// +optional
	AverageUsage map[InstanceManagerType]SampleWorkload `json:"averageUsage"`
	// An estimation of the maximum workload can handle
	// +optional
	EstimateMaximumWorkloads int `json:"estimateMaximumWorkloads"`
}

// The calculate average usage for all/per workload
type SampleWorkload struct {
	// The average usage including all workloads.
	// +optional
	All SampleResourceUsage `json:"all"`

	// The average usage of one workloads.
	// +optional
	One SampleResourceUsage `json:"one"`
}

// The CPU/Memory usage
type SampleResourceUsage struct {
	// The average CPU cores used in duration.
	// +optional
	CPUCores string `json:"cpuCores"`
	// The average memory used in duration.
	// +optional
	MemoryBytes string `json:"memoryBytes"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhsample
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`,description="The state"
// +kubebuilder:printcolumn:name="Guaranteed-CPU(%)",type=string,JSONPath=`.status.guaranteedInstanceManagerCPU.combine`,description="The guaranteed Instance manager CPU in setting"
// +kubebuilder:printcolumn:name="Average-CPU(cores)",type=string,JSONPath=`.status.averageUsage.combine.one.cpuCores`,description="The average instance manager CPU of single workload"
// +kubebuilder:printcolumn:name="Average-Mem(bytes)",type=string,JSONPath=`.status.averageUsage.combine.one.memoryBytes`,description="The average instance manager memory of single workload"
// +kubebuilder:printcolumn:name="Created-Workloads",type=integer,JSONPath=`.status.numberOfWorkloadsCreated`,description="The number of workloads created"
// +kubebuilder:printcolumn:name="Duration(minutes)",type=integer,JSONPath=`.spec.durationInMinutes`,description="The collect duration"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Sample object.
type Sample struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SampleSpec   `json:"spec,omitempty"`
	Status SampleStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// List of Sample objects.
type SampleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Sample `json:"items"`
}
