package types

const (
	LonghornManagerDaemonSetName = "longhorn-manager"
	LonghornManagerContainerName = LonghornManagerDaemonSetName
	LonghornUIDeploymentName     = "longhorn-ui"

	DriverDeployerName = "longhorn-driver-deployer"
	CSIAttacherName    = "csi-attacher"
	CSIProvisionerName = "csi-provisioner"
	CSIResizerName     = "csi-resizer"
	CSISnapshotterName = "csi-snapshotter"
	CSIPluginName      = "longhorn-csi-plugin"
)
