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

package v1beta1

import (
	longhornv1beta1 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	labels "k8s.io/apimachinery/pkg/labels"
	listers "k8s.io/client-go/listers"
	cache "k8s.io/client-go/tools/cache"
)

// BackingImageManagerLister helps list BackingImageManagers.
// All objects returned here must be treated as read-only.
type BackingImageManagerLister interface {
	// List lists all BackingImageManagers in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*longhornv1beta1.BackingImageManager, err error)
	// BackingImageManagers returns an object that can list and get BackingImageManagers.
	BackingImageManagers(namespace string) BackingImageManagerNamespaceLister
	BackingImageManagerListerExpansion
}

// backingImageManagerLister implements the BackingImageManagerLister interface.
type backingImageManagerLister struct {
	listers.ResourceIndexer[*longhornv1beta1.BackingImageManager]
}

// NewBackingImageManagerLister returns a new BackingImageManagerLister.
func NewBackingImageManagerLister(indexer cache.Indexer) BackingImageManagerLister {
	return &backingImageManagerLister{listers.New[*longhornv1beta1.BackingImageManager](indexer, longhornv1beta1.Resource("backingimagemanager"))}
}

// BackingImageManagers returns an object that can list and get BackingImageManagers.
func (s *backingImageManagerLister) BackingImageManagers(namespace string) BackingImageManagerNamespaceLister {
	return backingImageManagerNamespaceLister{listers.NewNamespaced[*longhornv1beta1.BackingImageManager](s.ResourceIndexer, namespace)}
}

// BackingImageManagerNamespaceLister helps list and get BackingImageManagers.
// All objects returned here must be treated as read-only.
type BackingImageManagerNamespaceLister interface {
	// List lists all BackingImageManagers in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*longhornv1beta1.BackingImageManager, err error)
	// Get retrieves the BackingImageManager from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*longhornv1beta1.BackingImageManager, error)
	BackingImageManagerNamespaceListerExpansion
}

// backingImageManagerNamespaceLister implements the BackingImageManagerNamespaceLister
// interface.
type backingImageManagerNamespaceLister struct {
	listers.ResourceIndexer[*longhornv1beta1.BackingImageManager]
}
