package csi

import (
	"context"
	"fmt"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/kubernetes/pkg/util/mount"
	"os"
	"path/filepath"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	"github.com/longhorn/longhorn-manager/types"
)

// NodeServer object contains
// RancherClient, nodeID and
// service capability
type NodeServer struct {
	apiClient *longhornclient.RancherClient
	nodeID    string
	caps      []*csi.NodeServiceCapability
}

// NewNodeServer returns a new NodeServer
func NewNodeServer(apiClient *longhornclient.RancherClient, nodeID string) *NodeServer {
	return &NodeServer{
		apiClient: apiClient,
		nodeID:    nodeID,
		caps: getNodeServiceCapabilities(
			[]csi.NodeServiceCapability_RPC_Type{
				csi.NodeServiceCapability_RPC_EXPAND_VOLUME,
			}),
	}
}

// NodePublishVolume will mount the volume /dev/longhorn/<volume_name> to target_path.
// This will format the device if the given request volume capability is mount and not
// match the requested fstype.
// Returns error when:
// * the existing volume has more than one controllers
// * the existing volume state is not attached
// * the existing volume frontend is disabled
// * the existing volume frontend type is not blockdev
// * the existing volume controller has no endpoint
// * the existing volume is not ready
func (ns *NodeServer) NodePublishVolume(ctx context.Context, req *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	logrus.Infof("NodeServer NodePublishVolume req: %v", req)

	existVol, err := ns.apiClient.Volume.ById(req.GetVolumeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if existVol == nil {
		msg := fmt.Sprintf("NodePublishVolume: the volume %s not exists", req.GetVolumeId())
		logrus.Warn(msg)
		return nil, status.Error(codes.NotFound, msg)
	}

	if len(existVol.Controllers) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "There should be only one controller for volume %s", req.GetVolumeId())
	}
	if existVol.State != string(types.VolumeStateAttached) || existVol.DisableFrontend ||
		existVol.Frontend != string(types.VolumeFrontendBlockDev) || existVol.Controllers[0].Endpoint == "" {
		return nil, status.Errorf(codes.InvalidArgument, "There is no block device frontend for volume %s", req.GetVolumeId())
	}
	if !existVol.Ready {
		return nil, status.Errorf(codes.Aborted, "The attached volume %s should be ready for workloads before the mount", req.GetVolumeId())
	}

	readOnly := req.GetReadonly()
	if readOnly {
		return nil, status.Error(codes.FailedPrecondition, "Not support readOnly")
	}

	targetPath := req.GetTargetPath()
	devicePath := existVol.Controllers[0].Endpoint
	diskMounter := &mount.SafeFormatAndMount{Interface: mount.New(""), Exec: mount.NewOsExec()}
	vc := req.GetVolumeCapability()
	if vc == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not provided")
	}

	if blkCapability := vc.GetBlock(); blkCapability != nil {
		return ns.nodePublishBlockVolume(req.GetVolumeId(), devicePath, targetPath, diskMounter)
	} else if mntCapability := vc.GetMount(); mntCapability != nil {
		userExt4Params, _ := ns.apiClient.Setting.ById(string(types.SettingNameMkfsExt4Parameters))

		// mounter assumes ext4 by default
		fsType := vc.GetMount().GetFsType()
		if fsType == "" {
			fsType = "ext4"
		}

		// we allow the user to provide additional params for ext4 filesystem creation.
		// this allows an ext4 fs to be mounted on older kernels, see https://github.com/longhorn/longhorn/issues/1208
		if fsType == "ext4" && userExt4Params != nil && userExt4Params.Value != "" {
			ext4Params := userExt4Params.Value
			logrus.Infof("enabling user provided ext4 fs creation params: %s for volume: %s", ext4Params, req.GetVolumeId())
			cmdParamMapping := map[string]string{"mkfs." + fsType: ext4Params}
			diskMounter = &mount.SafeFormatAndMount{
				Interface: mount.New(""),
				Exec:      NewForcedParamsOsExec(cmdParamMapping),
			}
		}

		return ns.nodePublishMountVolume(req.GetVolumeId(), devicePath, targetPath,
			fsType, vc.GetMount().GetMountFlags(), diskMounter)
	}

	return nil, status.Error(codes.InvalidArgument, "Invalid volume capability, neither Mount nor Block")
}

// nodePublishMountVolume mounts the given device path to the given target path
// using fsck, mkfs, mount, and returns a new csi.NodePublishVolumeResponse object
// if successfully mounted.
// Returns error when the given target path is already mounted
func (ns *NodeServer) nodePublishMountVolume(volumeName, devicePath, targetPath, fsType string, mountFlags []string, mounter *mount.SafeFormatAndMount) (*csi.NodePublishVolumeResponse, error) {
	// It's used to check if a directory is a mount point and it will create the directory if not exist. Hence this target path cannot be used for block volume.
	notMnt, err := isLikelyNotMountPointAttach(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !notMnt {
		logrus.Debugf("NodePublishVolume: the volume %s has been mounted", volumeName)
		return &csi.NodePublishVolumeResponse{}, nil
	}

	if err := mounter.FormatAndMount(devicePath, targetPath, fsType, mountFlags); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	logrus.Debugf("NodePublishVolume: done MountVolume %s", volumeName)

	return &csi.NodePublishVolumeResponse{}, nil
}

// nodePublishBlockVolume mounts the device to the given namespace
// path, and returns a new empty csi.NodePublishVolumeResponse if 
// successfully mounted.
// It does:
// * mount to host targetDir namespace if exist or create new
// * create empty targetPath in the targetDir namespace
// * mount the existing device path to the targetPath
func (ns *NodeServer) nodePublishBlockVolume(volumeName, devicePath, targetPath string, mounter *mount.SafeFormatAndMount) (*csi.NodePublishVolumeResponse, error) {
	targetDir := filepath.Dir(targetPath)
	exists, err := mounter.ExistsPath(targetDir)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if !exists {
		if err := mounter.MakeDir(targetDir); err != nil {
			return nil, status.Errorf(codes.Internal, "Could not create dir %q: %v", targetDir, err)
		}
	}
	if err = mounter.MakeFile(targetPath); err != nil {
		return nil, status.Errorf(codes.Internal, "Error in making file %v", err)
	}

	if err := mounter.Mount(devicePath, targetPath, "", []string{"bind"}); err != nil {
		if removeErr := os.Remove(targetPath); removeErr != nil {
			return nil, status.Errorf(codes.Internal, "Could not remove mount target %q: %v", targetPath, err)
		}
		return nil, status.Errorf(codes.Internal, "Could not mount %q at %q: %v", devicePath, targetPath, err)
	}
	logrus.Debugf("NodePublishVolume: done BlockVolume %s", volumeName)

	return &csi.NodePublishVolumeResponse{}, nil
}

// NodeUnpublishVolume execute unmount or use os.remove for the
// requested target path. Returns error when the given request 
// target path is not a mount point
func (ns *NodeServer) NodeUnpublishVolume(ctx context.Context, req *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	logrus.Infof("NodeServer NodeUnpublishVolume req: %v", req)

	targetPath := req.GetTargetPath()

	notMnt, err := isLikelyNotMountPointDetach(targetPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if notMnt {
		return nil, status.Error(codes.NotFound, "Volume not mounted")
	}

	mounter := mount.New("")
	for {
		if err := mounter.Unmount(targetPath); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		notMnt, err := mounter.IsLikelyNotMountPoint(targetPath)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if notMnt {
			break
		}
		logrus.Debugf("There are multiple mount layers on mount point %v, will unmount all mount layers for this mount point", targetPath)
	}

	if err := mount.CleanupMountPoint(targetPath, mounter, false); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	logrus.Debugf("NodeUnpublishVolume: done %s", req.GetVolumeId())

	return &csi.NodeUnpublishVolumeResponse{}, nil
}

// NodeStageVolume not implemented
func (ns *NodeServer) NodeStageVolume(
	ctx context.Context,
	req *csi.NodeStageVolumeRequest) (
	*csi.NodeStageVolumeResponse, error) {

	return nil, status.Error(codes.Unimplemented, "")
}

// NodeUnstageVolume not implemented
func (ns *NodeServer) NodeUnstageVolume(
	ctx context.Context,
	req *csi.NodeUnstageVolumeRequest) (
	*csi.NodeUnstageVolumeResponse, error) {

	return nil, status.Error(codes.Unimplemented, "")
}

// NodeGetVolumeStats not implemented
func (ns *NodeServer) NodeGetVolumeStats(ctx context.Context, in *csi.NodeGetVolumeStatsRequest) (*csi.NodeGetVolumeStatsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "")
}

// NodeExpandVolume is designed to expand the file system for ONLINE expansion,
// But Longhorn supports OFFLINE expansion only.
func (ns *NodeServer) NodeExpandVolume(ctx context.Context, req *csi.NodeExpandVolumeRequest) (*csi.NodeExpandVolumeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "")
}

// NodeGetInfo returns a new csi.NodeGetInfoResponse object
func (ns *NodeServer) NodeGetInfo(ctx context.Context, req *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return &csi.NodeGetInfoResponse{
		NodeId: ns.nodeID,
	}, nil
}

// NodeGetCapabilities returns a new csi.NodeGetCapabilitiesResponse
// object
func (ns *NodeServer) NodeGetCapabilities(ctx context.Context, req *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	return &csi.NodeGetCapabilitiesResponse{
		Capabilities: ns.caps,
	}, nil
}

// getNodeServiceCapabilities returns a list of csi.NodeServiceCapability
// for the given csi.NodeServiceCapability_RPC_Type
func getNodeServiceCapabilities(cs []csi.NodeServiceCapability_RPC_Type) []*csi.NodeServiceCapability {
	var nscs []*csi.NodeServiceCapability

	for _, cap := range cs {
		logrus.Infof("Enabling node service capability: %v", cap.String())
		nscs = append(nscs, &csi.NodeServiceCapability{
			Type: &csi.NodeServiceCapability_Rpc{
				Rpc: &csi.NodeServiceCapability_RPC{
					Type: cap,
				},
			},
		})
	}

	return nscs
}
