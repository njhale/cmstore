package cmstore

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"reflect"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	prefix   = "cmstore"
	objKey   = prefix + ".obj"
	labelKey = prefix + "/key"
)

// type Partitioner interface {
// 	Partition(obj runtime.Object) (partitions <-chan runtime.Object, err error)
// 	Join(partitions chan<- runtime.Object) (obj runtime.Object, err error)
// }

// type Partitions interface {
// 	Push(partitions ...runtime.Object) error
// 	Pop() (partition runtime.Object, err error)
// }

// type simplePartitions struct {
// 	partitions []runtime.Object
//     top int
// }

type Object interface {
	runtime.Object
	metav1.Object
}

// type Partitioner interface {
// 	Partition(ctx context.Context, from Object) (partitions []Object, err error)
// 	Join(ctx context.Context, into Object, partitions ...Object) error
// 	NewPartition() runtime.Object
// 	NewPartitionList() runtime.Object
// }

type ConfigMapPartitioner struct {
	codec runtime.Codec
}

func (p *ConfigMapPartitioner) Partition(_ context.Context, from Object) (partitions []Object, err error) {
	// TODO(njhale): partition across several configmaps
	var data []byte
	if data, err = runtime.Encode(p.codec, from); err != nil {
		return
	}

	cm := &corev1.ConfigMap{
		BinaryData: map[string][]byte{
			objKey: data,
		},
	}
	cm.SetGenerateName("partition-")

	partitions = append(partitions, cm)

	return
}

func (p *ConfigMapPartitioner) Join(_ context.Context, into Object, partitions ...Object) error {
	// Sort partitions by
	sorted, err := sortPartitions(partitions)
	if err != nil {
		return fmt.Errorf("error joining partitions: %s", err)
	}

	if len(sorted) < 1 {
		return fmt.Errorf("no partitions to join")
	}

	// Join partitions
	var (
		rvBuilder strings.Builder
		data      []byte
	)
	for i, partition := range sorted {
		fmt.Fprintf(&rvBuilder, partition.GetResourceVersion())
		cm, ok := partition.(*corev1.ConfigMap)
		if !ok {
			return fmt.Errorf("failed to join partition %d, expected ConfigMap got %t", i, partition)
		}
		data = append(data, partition)
	}

	decoded, _, err := p.codec.Decode(data, nil, into)
	if err != nil {
		return fmt.Errorf("error decoding joined partitions: %s", err)
	}

	if decoded == nil {
		return fmt.Errorf("failed to decode joined partitions")
	}

	// TODO(njhale): Make the combined resource version more standard looking
	decoded.SetResourceVersion(safeHash32(rvBuilder.String()))
	// TODO(njhale): Hash UID to reduce chance of GC collision
	decoded.SetUID(sorted[0].GetUID())

	reflect.ValueOf(into).Elem().Set(reflect.ValueOf(decoded).Elem())

	return nil
}

func sortPartitions(partitions []Object) ([]Object, error) {
	var (
		length = len(partitions)
		sorted = make([]Object, len(partitions))
	)
	for _, partition := range partitions {
		// Panic, indicating programmer error
		if partition == nil {
			panic("programmer error: nil partition found")
		}

		position, found := paritionPosition(partition)
		if !found {
			return nil, fmt.Errorf("couldn't locate partition position: %v", partition)
		}

		if position < 0 || position >= length {
			return nil, fmt.Errorf("partition position %d out of bounds [0, %d)", position, length)
		}

		if sorted[position] != nil {
			return nil, fmt.Errorf("duplicate partition at position %d: %v", position, partition)
		}

		sorted[position] = position
	}

	return sorted, nil
}

func partitionPosition(paritionMeta metav1.Object) (position int, found bool) {
	annotations := partitionMeta.GetAnnotations()
	if len(annotations) < 1 {
		return
	}

	var p string
	if p, found = annotations[partitionAnnotationKey]; !found {
		return
	}

	var err error
	if position, err = strconv.Atoi(p); err != nil {
		found = false
	}

	return
}

func safeHash32(s string) (string, error) {
	hasher := fnv.New32a()
	if _, err := hasher.Write([]byte(s)); err != nil {
		return "", nil
	}

	var sum []byte
	sum = hasher.Sum(sum)

	return rand.SafeEncodeString(string(sum)), nil
}

type ConfigMapStore struct {
	client           client.Client
	versioner        storage.Versioner
	partitioner      Partitioner
	storageNamespace string
}

// Stamp applies the store labels and namespace to a predicate.
func (s *ConfigMapStore) stamp(key string, predicate Object) {
	labels := predicate.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	labels[labelKey] = key
	predicate.SetLabels(labels)

	predicate.SetNamespace(s.storageNamespace)
}

func (s *ConfigMapStore) labelSelector(key string) *metav1.LabelSelector {
	return &metav1.LabelSelector{
		MatchLabels: map[string]string{
			labelKey: key,
		},
	}
}

var _ storage.Interface = &ConfigMapStore{}

func (s *ConfigMapStore) Versioner() storage.Versioner {
	return s.versioner
}

func (s *ConfigMapStore) Create(ctx context.Context, key string, obj, out runtime.Object, _ uint64) error {
	if version, err := s.versioner.ObjectResourceVersion(obj); err == nil && version != 0 {
		return errors.New("resourceVersion should not be set on objects to be created")
	}
	if err := s.versioner.PrepareObjectForStorage(obj); err != nil {
		return fmt.Errorf("PrepareObjectForStorage failed: %v", err)
	}

	o, ok := obj.(Object)
	if !ok {
		// TODO(njhale): return appropriate storage error
		return fmt.Errorf("assertion failed: %t is not an Object", obj)
	}

	existing := o.DeepCopyObject().(Object)
	if err := s.Get(ctx, key, "", existing, true); err != nil {
		// TODO(njhale): return appropriate storage error
		return err
	}
	if existing.GetUID() != "" {
		// TODO(njhale): return appropriate storage error
		return fmt.Errorf("resource already exists")
	}

	partitions, err := s.partitioner.Partition(ctx, o)
	if err != nil {
		// TODO(njhale): convert to appropriate storage error
		return err
	}

	if len(partitions) < 1 {
		// TODO(njhale): convert to appropriate storage error
		return fmt.Errorf("failed to partition")
	}

	// TODO(njhale): parallelize
	defer func() {
		if err == nil {
			// Transaction was successful!
			return
		}

		// Cancel transaction
		for _, p := range partitions {
			if p.GetUID() == "" {
				// Since paritions are created sequentially, we know later elements were never created, so we can bail out early
				return
			}

			// TODO(njhale): log errors
			s.client.Delete(ctx, p)
		}

		return
	}()

	for i, p := range partitions {
		s.stamp(key, p)
		// TODO(njhale): chain OwnerReferences
		if err = s.client.Create(ctx, p); err != nil {
			// TODO(njhale): return appropriate storage error
			break
		}

		partitions[i] = p
	}

	// Project metadata from the first partition onto the result
	// We already know that more than one partition exists, so it's okay to index directly
	var accessor metav1.Object
	accessor, err = meta.Accessor(obj)
	if err != nil {
		// TODO(njhale): return appropriate storage error
		return err
	}
	ProjectMeta(partitions[0], accessor)

	reflect.ValueOf(out).Elem().Set(reflect.ValueOf(obj).Elem())

	return err
}

func (s *ConfigMapStore) Delete(ctx context.Context, key string, out runtime.Object, preconditions *storage.Preconditions, validateDeletion storage.ValidateObjectFunc) error {
	// TODO(njhale): implement
	panic("not implemented")
	return nil
}

func (s *ConfigMapStore) Watch(ctx context.Context, key, resourceVersion string, p storage.SelectionPredicate) (watch.Interface, error) {
	// TODO(njhale): implement
	panic("not implemented")
	return nil, nil
}

func (s *ConfigMapStore) WatchList(ctx context.Context, key, resourceVersion string, p storage.SelectionPredicate) (watch.Interface, error) {
	// TODO(njhale): implement
	panic("not implemented")
	return nil, nil
}

func (s *ConfigMapStore) Get(ctx context.Context, key, resourceVersion string, obj runtime.Object, ignoreNotFound bool) error {
	partitionList := s.partitioner.NewPartitionList()
	if err := s.client.List(ctx, partitionList, client.InNamespace(s.storageNamespace), client.MatchingLabelsSelector{s.labelSelector(key)}); err != nil {
		// TODO(njhale): return appropriate storage error
		return err
	}

	var partitions []Object
	for _, obj := range partitionList.Items {
		partitions = append(partitions, obj.Object)
	}
	return nil
}

func (s *ConfigMapStore) GetToList(ctx context.Context, key, resourceVersion string, p storage.SelectionPredicate, list runtime.Object) error {
	// TODO(njhale): implement
	panic("not implemented")
	return nil
}

func (s *ConfigMapStore) List(ctx context.Context, key, resourceVersion string, p storage.SelectionPredicate, list runtime.Object) error {
	// TODO(njhale): implement
	panic("not implemented")
	return nil
}

func (s *ConfigMapStore) GuaranteedUpdate(ctx context.Context, key string, ptrToType runtime.Object, ignoreNotFound bool, preconditions *storage.Preconditions, tryUpdate storage.UpdateFunc, suggestion ...runtime.Object) error {
	// TODO(njhale): implement
	panic("not implemented")
	return nil
}

func (s *ConfigMapStore) Count(key string) (int64, error) {
	// TODO(njhale): implement
	panic("not implemented")
	return 0, nil
}

func ProjectMeta(from, to metav1.Object) {
	to.SetUID(from.GetUID())
	to.SetResourceVersion(from.GetResourceVersion())
}
