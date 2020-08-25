package api

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

// NodeList gets all manager pod nodes and respond in
// *client.GenericCollection Data
func (s *Server) NodeList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	nodeList, err := s.nodeList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(nodeList)
	return nil
}

// nodeList get all manager pod node IP mapped to node name from datastore cache,
func (s *Server) nodeList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	nodeList, err := s.m.ListNodesSorted()
	if err != nil {
		return nil, errors.Wrap(err, "fail to list nodes")
	}
	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return nil, errors.Wrap(err, "fail to get node ip")
	}
	return toNodeCollection(nodeList, nodeIPMap, apiContext), nil
}

// NodeGet get a node IP and its manager pod IP for the
// given node name in the request
func (s *Server) NodeGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)
	id := mux.Vars(req)["name"]

	node, err := s.m.GetNode(id)
	if err != nil {
		return errors.Wrapf(err, "fail to get node %v", id)
	}
	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to get node ip")
	}
	apiContext.Write(toNodeResource(node, nodeIPMap[node.Name], apiContext))
	return nil
}

// NodeUpdate update longhorn Node resource with lhclient for the given
// request
func (s *Server) NodeUpdate(rw http.ResponseWriter, req *http.Request) error {
	var n Node
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&n); err != nil {
		return err
	}

	id := mux.Vars(req)["name"]

	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to get node ip")
	}
	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		node, err := s.m.GetNode(id)
		if err != nil {
			return nil, err
		}
		node.Spec.AllowScheduling = n.AllowScheduling
		node.Spec.Tags = n.Tags

		return s.m.UpdateNode(node)
	})
	if err != nil {
		return err
	}
	unode, ok := obj.(*longhorn.Node)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to node %v object", id)
	}

	apiContext.Write(toNodeResource(unode, nodeIPMap[id], apiContext))
	return nil
}

// DiskUpdate updates longhorn Disk resource with lhclinet for the
// given request
func (s *Server) DiskUpdate(rw http.ResponseWriter, req *http.Request) error {
	var diskUpdate DiskUpdateInput
	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&diskUpdate); err != nil {
		return err
	}

	id := mux.Vars(req)["name"]

	nodeIPMap, err := s.m.GetManagerNodeIPMap()
	if err != nil {
		return errors.Wrap(err, "fail to get node ip")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.DiskUpdate(id, diskUpdate.Disks)
	})
	if err != nil {
		return err
	}
	unode, ok := obj.(*longhorn.Node)
	if !ok {
		return fmt.Errorf("BUG: cannot convert to node %v object", id)
	}
	apiContext.Write(toNodeResource(unode, nodeIPMap[id], apiContext))
	return nil
}

// NodeDelete deletes longhorn Node resource with lhclient for
// the given request
func (s *Server) NodeDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	if err := s.m.DeleteNode(id); err != nil {
		return errors.Wrap(err, "unable to delete node")
	}

	return nil
}
