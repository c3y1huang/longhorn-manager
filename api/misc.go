package api

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/manager"
)

// EventList gets longhorn event list for the request
func (s *Server) EventList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	eventList, err := s.eventList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(eventList)
	return nil
}

// eventList get a list of longhorn events with kubeclient,
// and returns in *client.GenericCollection Data
func (s *Server) eventList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	eventList, err := s.m.GetLonghornEventList()
	if err != nil {
		return nil, errors.Wrap(err, "fail to list events")
	}
	return toEventCollection(eventList), nil
}

// InitiateSupportBundle generates support bundle for the given
// request
func (s *Server) InitiateSupportBundle(w http.ResponseWriter, req *http.Request) error {
	var sb *manager.SupportBundle
	var supportBundleInput SupportBundleInitateInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&supportBundleInput); err != nil {
		return err
	}
	sb, err := s.m.InitSupportBundle(supportBundleInput.IssueURL, supportBundleInput.Description)
	if err != nil {
		return fmt.Errorf("unable to initiate Support Bundle Download:%v", err)
	}
	apiContext.Write(toSupportBundleResource(s.m.GetCurrentNodeID(), sb))
	return nil
}

// QuerySupportBundle get the support bundle from the cached datastore
// for the given request
func (s *Server) QuerySupportBundle(w http.ResponseWriter, req *http.Request) error {
	bundleName := mux.Vars(req)["bundleName"]
	apiContext := api.GetApiContext(req)
	sb, err := s.m.GetSupportBundle(bundleName)
	if err != nil {
		return errors.Wrap(err, "failed to get support bundle")
	}
	apiContext.Write(toSupportBundleResource(s.m.GetCurrentNodeID(), sb))
	return nil
}

// DownloadSupportBundle returns the support bundle in response stream
func (s *Server) DownloadSupportBundle(w http.ResponseWriter, req *http.Request) error {
	bundleName := mux.Vars(req)["bundleName"]
	sb, err := s.m.GetSupportBundle(bundleName)
	if err != nil {
		return err
	}

	if sb.State == manager.BundleStateError {
		return errors.Wrap(err, "support bundle creation failed")
	}
	if sb.State == manager.BundleStateInProgress {
		return fmt.Errorf("support bundle creation is still in progress")
	}

	file, err := s.m.GetBundleFileHandler()
	if err != nil {
		return err
	}
	defer file.Close()

	w.Header().Set("Content-Disposition", "attachment; filename="+sb.Filename)
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Length", strconv.FormatInt(sb.Size, 10))
	if _, err := io.Copy(w, file); err != nil {
		return err
	}
	return nil
}

// DiskTagList get node disk tags in datastore cache for the given request
func (s *Server) DiskTagList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	tags, err := s.m.GetDiskTags()
	if err != nil {
		return errors.Wrap(err, "failed to get all tags")
	}

	apiContext.Write(toTagCollection(tags, "disk", apiContext))
	return nil
}

// NodeTagList get node tags in datastore cache for the given request
func (s *Server) NodeTagList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	tags, err := s.m.GetNodeTags()
	if err != nil {
		return errors.Wrap(err, "failed to get all tags")
	}

	apiContext.Write(toTagCollection(tags, "node", apiContext))
	return nil
}

// InstanceManagerGet get the InstanceManager object for the given name in
// request
func (s *Server) InstanceManagerGet(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	apiContext := api.GetApiContext(req)

	im, err := s.m.GetInstanceManager(id)
	if err != nil {
		return err
	}

	apiContext.Write(toInstanceManagerResource(im))
	return nil
}

// InstanceManagerList get a list of instance managers in datastore cache,
// and respond in *client.GenericCollection Data
func (s *Server) InstanceManagerList(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	instanceManagers, err := s.m.ListInstanceManagers()
	if err != nil {
		return errors.Wrap(err, "failed to list instance managers")
	}

	apiContext.Write(toInstanceManagerCollection(instanceManagers))
	return nil
}
