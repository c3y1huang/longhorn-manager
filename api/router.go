package api

import (
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
)

type HandleFuncWithError func(http.ResponseWriter, *http.Request) error

const DefaultPort int = 9500

func HandleError(s *client.Schemas, t HandleFuncWithError) http.Handler {
	return api.ApiHandler(s, http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if err := t(rw, req); err != nil {
			logrus.Warnf("HTTP handling error %v", err)
			apiContext := api.GetApiContext(req)
			apiContext.WriteErr(err)
		}
	}))
}

func NewRouter(s *Server) *mux.Router {
	schemas := NewSchema()
	r := mux.NewRouter().StrictSlash(true)
	f := HandleError

	versionsHandler := api.VersionsHandler(schemas, "v1")
	versionHandler := api.VersionHandler(schemas, "v1")
	r.Methods("GET").Path("/").Handler(versionsHandler)
	r.Methods("GET").Path("/v1").Handler(versionHandler)
	r.Methods("GET").Path("/v1/apiversions").Handler(versionsHandler)
	r.Methods("GET").Path("/v1/apiversions/v1").Handler(versionHandler)
	r.Methods("GET").Path("/v1/schemas").Handler(api.SchemasHandler(schemas))
	r.Methods("GET").Path("/v1/schemas/{id}").Handler(api.SchemaHandler(schemas))

	//r.Methods("GET").Path("/v1/settings").Handler(f(schemas, s.settings.List))
	//r.Methods("GET").Path("/v1/settings/{name}").Handler(f(schemas, s.settings.Get))
	//r.Methods("PUT").Path("/v1/settings/{name}").Handler(f(schemas, s.settings.Set))

	r.Methods("GET").Path("/v1/volumes").Handler(f(schemas, s.VolumeList))
	r.Methods("GET").Path("/v1/volumes/{name}").Handler(f(schemas, s.VolumeGet))
	r.Methods("DELETE").Path("/v1/volumes/{name}").Handler(f(schemas, s.VolumeDelete))
	r.Methods("POST").Path("/v1/volumes").Handler(f(schemas, s.VolumeCreate))

	volumeActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"attach":         s.VolumeAttach,
		"detach":         s.VolumeDetach,
		"snapshotPurge":  s.SnapshotPurge,
		"snapshotCreate": s.SnapshotCreate,
		"snapshotList":   s.SnapshotList,
		"snapshotGet":    s.SnapshotGet,
		"snapshotDelete": s.SnapshotDelete,
		"snapshotRevert": s.SnapshotRevert,
		//"snapshotBackup": s.fwd.Handler(HostIDFromVolume(s.man), s.snapshots.Backup),
		//"recurringUpdate": s.fwd.Handler(HostIDFromVolume(s.man), s.UpdateRecurring),
		//"bgTaskQueue":     s.fwd.Handler(HostIDFromVolume(s.man), s.BgTaskQueue),
		//"replicaRemove":   s.fwd.Handler(HostIDFromVolume(s.man), s.ReplicaRemove),
	}
	for name, action := range volumeActions {
		r.Methods("POST").Path("/v1/volumes/{name}").Queries("action", name).Handler(f(schemas, action))
	}

	//r.Methods("GET").Path("/v1/backupvolumes").Handler(f(schemas, s.backups.ListVolume))
	//r.Methods("GET").Path("/v1/backupvolumes/{volName}").Handler(f(schemas, s.backups.GetVolume))
	//backupActions := map[string]func(http.ResponseWriter, *http.Request) error{
	//	"backupList":   s.backups.List,
	//	"backupGet":    s.backups.Get,
	//	"backupDelete": s.backups.Delete,
	//}
	//for name, action := range backupActions {
	//	r.Methods("POST").Path("/v1/backupvolumes/{volName}").Queries("action", name).Handler(f(schemas, action))
	//}

	r.Methods("GET").Path("/v1/hosts").Handler(f(schemas, s.NodeList))
	r.Methods("GET").Path("/v1/hosts/{id}").Handler(f(schemas, s.NodeGet))

	return r
}
