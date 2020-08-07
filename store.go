package cmstore

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

type ConfigMapStore struct {
	versioner storage.Versioner
}

var _ storage.Interface = &ConfigMapStore{}

func (c *ConfigMapStore) Versioner() storage.Versioner {
	return c.versioner
}

func (c *ConfigMapStore) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	// TODO(njhale): implement
	panic("not implemented")
	return nil
}

func (c *ConfigMapStore) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc) error {
	// TODO(njhale): implement
	panic("not implemented")
	return nil
}

func (c *ConfigMapStore) Watch(ctx context.Context, key, resourceVersion string, p storage.SelectionPredicate) (watch.Interface, error) {
	// TODO(njhale): implement
	panic("not implemented")
	return nil, nil
}

func (c *ConfigMapStore) WatchList(ctx context.Context, key, resourceVersion string, p storage.SelectionPredicate) (watch.Interface, error) {
	// TODO(njhale): implement
	panic("not implemented")
	return nil, nil
}

func (c *ConfigMapStore) Get(ctx context.Context, key, resourceVersion string, obj runtime.Object, ignoreNotFound bool) error {
	// TODO(njhale): implement
	panic("not implemented")
	return nil
}

func (c *ConfigMapStore) GetToList(ctx context.Context, key, resourceVersion string, p storage.SelectionPredicate, list runtime.Object) error {
	// TODO(njhale): implement
	panic("not implemented")
	return nil
}

func (c *ConfigMapStore) List(ctx context.Context, key, resourceVersion string, p storage.SelectionPredicate, list runtime.Object) error {
	// TODO(njhale): implement
	panic("not implemented")
	return nil
}

func (c *ConfigMapStore) GuaranteedUpdate(ctx context.Context, key string, ptrToType runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, suggestion ...runtime.Object) error {
	// TODO(njhale): implement
	panic("not implemented")
	return nil
}

func (c *ConfigMapStore) Count(key string) (int64, error) {
	// TODO(njhale): implement
	panic("not implemented")
	return 0, nil
}
