package cmstore

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestCreate(t *testing.T) {
	store := &ConfigMapStore{}

	// Nested ConfigMaps, oh my!
	nsn := &types.NamespacedName{Namespace: "default", Name: "my-config"}
	in, out := &corev1.ConfigMap{}, &corev1.ConfigMap{}
	in.SetNamespace(nsn.Namespace)
	in.SetName(nsn.Name)

	ctx := context.Background()
	err := store.Create(ctx, nsn.String(), in, out, 0)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	// basic tests of the output
	if out.ObjectMeta.Name != in.ObjectMeta.Name {
		t.Errorf("pod name want=%s, get=%s", in.ObjectMeta.Name, out.ObjectMeta.Name)
	}
	if out.ResourceVersion == "" {
		t.Errorf("output should have non-empty resource version")
	}
	if out.SelfLink != "" {
		t.Errorf("output should have empty self link")
	}
}

func TestDelete(t *testing.T) {
}

func TestWatch(t *testing.T) {
}

func TestWatchList(t *testing.T) {
}

func TestGet(t *testing.T) {
}

func TestGetToList(t *testing.T) {
}

func TestList(t *testing.T) {
}

func TestGuaranteedUpdate(t *testing.T) {
}

func TestCount(t *testing.T) {
}
