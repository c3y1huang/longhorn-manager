package csi

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	longhornclient "github.com/longhorn/longhorn-manager/client"
)

// Manager object contains
// all CSI servers object
type Manager struct {
	ids *IdentityServer
	ns  *NodeServer
	cs  *ControllerServer
}

func init() {}

// GetCSIManager returns a new
// Manager object
func GetCSIManager() *Manager {
	return &Manager{}
}

// Run starts a new gRPC server that registers the IdentityServer, NodeServer, and ControllerServer.
// The NodeServer and ControllerServer contains RancherClient
func (m *Manager) Run(driverName, nodeID, endpoint, csiVersion, identityVersion, managerURL string) error {
	logrus.Infof("CSI Driver: %v csiVersion: %v", driverName, csiVersion)

	// Longhorn API Client
	clientOpts := &longhornclient.ClientOpts{Url: managerURL}
	apiClient, err := longhornclient.NewRancherClient(clientOpts)
	if err != nil {
		return errors.Wrap(err, "Failed to initialize Longhorn API client")
	}

	// Create GRPC servers
	m.ids = NewIdentityServer(driverName, identityVersion)
	m.ns = NewNodeServer(apiClient, nodeID)
	m.cs = NewControllerServer(apiClient, nodeID)
	s := NewNonBlockingGRPCServer()
	s.Start(endpoint, m.ids, m.cs, m.ns)
	s.Wait()

	return nil
}
