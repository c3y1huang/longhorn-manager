package api

import (
	"net/http"
	"net/http/httputil"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/manager"
)

// OwnerIDFunc declars signature
type OwnerIDFunc func(req *http.Request) (string, error)

// OwnerIDFromVolume returns the volume OwnerID for the given
// volume name in request
func OwnerIDFromVolume(m *manager.VolumeManager) func(req *http.Request) (string, error) {
	return func(req *http.Request) (string, error) {
		name := mux.Vars(req)["name"]
		volume, err := m.Get(name)
		if err != nil {
			return "", errors.Wrapf(err, "error getting volume '%s'", name)
		}
		if volume == nil {
			return "", nil
		}
		return volume.Status.OwnerID, nil
	}
}

// OwnerIDFromNode returns the node ID for the given request
func OwnerIDFromNode(m *manager.VolumeManager) func(req *http.Request) (string, error) {
	return func(req *http.Request) (string, error) {
		id := mux.Vars(req)["name"]
		return id, nil
	}
}

// NodeLocator interface
type NodeLocator interface {
	GetCurrentNodeID() string
	Node2APIAddress(nodeID string) (string, error)
}

// Fwd object
type Fwd struct {
	locator NodeLocator
	proxy   http.Handler
}

// NewFwd returns a new Fwd object
func NewFwd(locator NodeLocator) *Fwd {
	return &Fwd{
		locator: locator,
		proxy:   &httputil.ReverseProxy{Director: func(r *http.Request) {}},
	}
}

// Handler updates request for the datastore cached
func (f *Fwd) Handler(getNodeID OwnerIDFunc, h HandleFuncWithError) HandleFuncWithError {
	return func(w http.ResponseWriter, req *http.Request) error {
		nodeID, err := getNodeID(req)
		if err != nil {
			return errors.Wrap(err, "fail to get node ID")
		}
		if nodeID != "" && nodeID != f.locator.GetCurrentNodeID() {
			targetNode, err := f.locator.Node2APIAddress(nodeID)
			if err != nil {
				return errors.Wrapf(err, "cannot find node %v", nodeID)
			}
			if targetNode != req.Host {
				req.Host = targetNode
				req.URL.Host = targetNode
				req.URL.Scheme = "http"
				logrus.Debugf("Forwarding request to %v", targetNode)
				f.proxy.ServeHTTP(w, req)
				return nil
			}
		}
		return h(w, req)
	}
}
