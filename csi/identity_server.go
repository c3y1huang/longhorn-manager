package csi

import (
	"context"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

// IdentityServer object contains
// driver info
type IdentityServer struct {
	driverName string
	version    string
}

// NewIdentityServer returns a new IdentityServer with the given
// driver name and version
func NewIdentityServer(driverName, version string) *IdentityServer {
	return &IdentityServer{
		driverName: driverName,
		version:    version,
	}
}

// GetPluginInfo returns a new csi.GetPluginInfoResponse object 
// contains driver name and version in the IdentityServer
func (ids *IdentityServer) GetPluginInfo(ctx context.Context, req *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	// Vendor version should be consistent with Longhorn manager version.
	return &csi.GetPluginInfoResponse{
		Name:          ids.driverName,
		VendorVersion: ids.version,
	}, nil
}

// Probe returns a new empty csi.ProbeResponse object
func (ids *IdentityServer) Probe(ctx context.Context, req *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return &csi.ProbeResponse{}, nil
}

// GetPluginCapabilities returns new csi.GetPluginCapabilitiesResponse
// object for the plugin capability
func (ids *IdentityServer) GetPluginCapabilities(ctx context.Context, req *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	return &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
			{
				Type: &csi.PluginCapability_VolumeExpansion_{
					VolumeExpansion: &csi.PluginCapability_VolumeExpansion{
						Type: csi.PluginCapability_VolumeExpansion_OFFLINE,
					},
				},
			},
		},
	}, nil
}
