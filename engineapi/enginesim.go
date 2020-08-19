package engineapi

import (
	"fmt"
	"sync"

	"github.com/longhorn/longhorn-manager/types"
)

// EngineSimulatorRequest object
type EngineSimulatorRequest struct {
	VolumeName     string
	VolumeSize     int64
	ControllerAddr string
	ReplicaAddrs   []string
}

// EngineSimulatorCollection contains the engine simulators
type EngineSimulatorCollection struct {
	simulators map[string]*EngineSimulator
	mutex      *sync.Mutex
}

// NewEngineSimulatorCollection creates new EngineSimulatorCollection
func NewEngineSimulatorCollection() *EngineSimulatorCollection {
	return &EngineSimulatorCollection{
		simulators: map[string]*EngineSimulator{},
		mutex:      &sync.Mutex{},
	}
}

// CreateEngineSimulator creates new simulator in the EngineSimulatorCollection for the given
// request, and returns error when volume or replica already exist in the collection
func (c *EngineSimulatorCollection) CreateEngineSimulator(request *EngineSimulatorRequest) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.simulators[request.VolumeName] != nil {
		return fmt.Errorf("duplicate simulator with volume name %v already exists", request.VolumeName)
	}
	s := &EngineSimulator{
		volumeName:     request.VolumeName,
		volumeSize:     request.VolumeSize,
		controllerAddr: request.ControllerAddr,
		running:        true,
		replicas:       map[string]*Replica{},
		mutex:          &sync.RWMutex{},
	}
	for _, addr := range request.ReplicaAddrs {
		if err := s.ReplicaAdd(addr, false); err != nil {
			return err
		}
	}
	c.simulators[s.volumeName] = s
	return nil
}


// GetEngineSimulator returns the EngineSimulator for the given volume name.
// This returns error if volume not pre-exist in the collection
func (c *EngineSimulatorCollection) GetEngineSimulator(volumeName string) (*EngineSimulator, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.simulators[volumeName] == nil {
		return nil, fmt.Errorf("unable to find simulator with volume name %v", volumeName)
	}
	return c.simulators[volumeName], nil
}

// DeleteEngineSimulator marks the volume to not running and removes the volume from collection,
// This returns error if volume not pre-exist in the collection
func (c *EngineSimulatorCollection) DeleteEngineSimulator(volumeName string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.simulators[volumeName] == nil {
		return fmt.Errorf("unable to find simulator with volume name %v", volumeName)
	}
	// stop the references
	c.simulators[volumeName].running = false
	delete(c.simulators, volumeName)
	return nil
}

// NewEngineClient returns an EngineSimulator of the given volume name. 
// This returns error if no matching volume name found in the collection
func (c *EngineSimulatorCollection) NewEngineClient(request *EngineClientRequest) (EngineClient, error) {
	engine, err := c.GetEngineSimulator(request.VolumeName)
	if err != nil {
		return nil, fmt.Errorf("cannot find existing engine simulator for client")
	}
	return engine, nil
}

// EngineSimulator contains the volume info and running state for the volume
type EngineSimulator struct {
	volumeName     string
	volumeSize     int64
	controllerAddr string
	running        bool
	replicas       map[string]*Replica
	mutex          *sync.RWMutex
}

// Name returns volume name from the EngineSimulator
func (e *EngineSimulator) Name() string {
	return e.volumeName
}

// ReplicaList returns a single object of all the replicas in EngineSimulator
func (e *EngineSimulator) ReplicaList() (map[string]*Replica, error) {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	ret := map[string]*Replica{}
	for _, replica := range e.replicas {
		rep := *replica
		ret[replica.URL] = &rep
	}
	return ret, nil
}

// ReplicaAdd adds Replica info to the EngineSimulator for the given URL and node.
// This returns error if any of the existing replica in ERR mode or URL already exist
// in the EngineSimulator
func (e *EngineSimulator) ReplicaAdd(url string, isRestoreVolume bool) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	for name, replica := range e.replicas {
		if replica.Mode == types.ReplicaModeERR {
			return fmt.Errorf("replica %v is in ERR mode, cannot add new replica", name)
		}
	}
	if e.replicas[url] != nil {
		return fmt.Errorf("duplicate replica %v already exists", url)
	}
	e.replicas[url] = &Replica{
		URL:  url,
		Mode: types.ReplicaModeRW,
	}
	return nil
}

// ReplicaRemove deletes replica in EngineSimulator for the given URL. 
// This returns error if replica does not exist in EngineSimulator
func (e *EngineSimulator) ReplicaRemove(addr string) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.replicas[addr] == nil {
		return fmt.Errorf("unable to find replica %v", addr)
	}
	delete(e.replicas, addr)
	return nil
}

// SimulateStopReplica marks replica mode in EngineSimulator to ERR.
// This returns error if replica not exist in EngineSimulator
func (e *EngineSimulator) SimulateStopReplica(addr string) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.replicas[addr] == nil {
		return fmt.Errorf("unable to find replica %v", addr)
	}
	e.replicas[addr].Mode = types.ReplicaModeERR
	return nil
}

func (e *EngineSimulator) SnapshotCreate(name string, labels map[string]string) (string, error) {
	return "", fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) SnapshotList() (map[string]*types.Snapshot, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) SnapshotGet(name string) (*types.Snapshot, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) SnapshotDelete(name string) error {
	return fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) SnapshotRevert(name string) error {
	return fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) SnapshotPurge() error {
	return fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) SnapshotPurgeStatus() (map[string]*types.PurgeStatus, error) {
	return nil, fmt.Errorf("not implemented")
}

func (e *EngineSimulator) SnapshotBackup(snapName, backupTarget string, labels map[string]string, credential map[string]string) (string, error) {
	return "", fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) SnapshotBackupStatus() (map[string]*types.BackupStatus, error) {
	return nil, fmt.Errorf("not implemented")
}

func (e *EngineSimulator) Version(clientOnly bool) (*EngineVersion, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) Info() (*Volume, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) Endpoint() (string, error) {
	return "", fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) Expand(size int64) error {
	return fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) BackupRestore(backupTarget, backupName, backupVolume, lastRestored string, credential map[string]string) error {
	return fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) BackupRestoreStatus() (map[string]*types.RestoreStatus, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) ReplicaRebuildStatus() (map[string]*types.RebuildStatus, error) {
	return nil, fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) FrontendStart(volumeFrontend types.VolumeFrontend) error {
	return fmt.Errorf("Not implemented")
}
func (e *EngineSimulator) FrontendShutdown() error {
	return fmt.Errorf("Not implemented")
}

func (e *EngineSimulator) ReplicaRebuildVerify(url string) error {
	return fmt.Errorf("Not implemented")
}
