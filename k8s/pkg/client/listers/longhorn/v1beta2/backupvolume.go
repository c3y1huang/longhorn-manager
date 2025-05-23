/*
Copyright The Longhorn Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by lister-gen. DO NOT EDIT.

package v1beta2

import (
	longhornv1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	labels "k8s.io/apimachinery/pkg/labels"
	listers "k8s.io/client-go/listers"
	cache "k8s.io/client-go/tools/cache"
)

// BackupVolumeLister helps list BackupVolumes.
// All objects returned here must be treated as read-only.
type BackupVolumeLister interface {
	// List lists all BackupVolumes in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*longhornv1beta2.BackupVolume, err error)
	// BackupVolumes returns an object that can list and get BackupVolumes.
	BackupVolumes(namespace string) BackupVolumeNamespaceLister
	BackupVolumeListerExpansion
}

// backupVolumeLister implements the BackupVolumeLister interface.
type backupVolumeLister struct {
	listers.ResourceIndexer[*longhornv1beta2.BackupVolume]
}

// NewBackupVolumeLister returns a new BackupVolumeLister.
func NewBackupVolumeLister(indexer cache.Indexer) BackupVolumeLister {
	return &backupVolumeLister{listers.New[*longhornv1beta2.BackupVolume](indexer, longhornv1beta2.Resource("backupvolume"))}
}

// BackupVolumes returns an object that can list and get BackupVolumes.
func (s *backupVolumeLister) BackupVolumes(namespace string) BackupVolumeNamespaceLister {
	return backupVolumeNamespaceLister{listers.NewNamespaced[*longhornv1beta2.BackupVolume](s.ResourceIndexer, namespace)}
}

// BackupVolumeNamespaceLister helps list and get BackupVolumes.
// All objects returned here must be treated as read-only.
type BackupVolumeNamespaceLister interface {
	// List lists all BackupVolumes in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*longhornv1beta2.BackupVolume, err error)
	// Get retrieves the BackupVolume from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*longhornv1beta2.BackupVolume, error)
	BackupVolumeNamespaceListerExpansion
}

// backupVolumeNamespaceLister implements the BackupVolumeNamespaceLister
// interface.
type backupVolumeNamespaceLister struct {
	listers.ResourceIndexer[*longhornv1beta2.BackupVolume]
}
