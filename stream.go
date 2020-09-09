package cmstore

import (
	"bytes"
	"context"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	streamPrefix = "stream.x-k8s.io"
	streamObjKey = streamPrefix + ".obj"
	labelKey     = streamPrefix + "/key"
)

func NewStream(client client.Client, namespace, label string) *ConfigMapStream {
	return &ConfigMapStream{
		Client:    client,
		label:     label,
		namespace: namespace,
	}
}

type ConfigMapStream struct {
	Client client.Client

	elements  []corev1.ConfigMap
	current   int
	offset    int64
	label     string
	namespace string
}

// Write adds a ConfigMap containing p to the stream.
func (s *ConfigMapStream) Write(p []byte) (n int, err error) {
	l := len(p)
	if l < 1 {
		return 0, fmt.Errorf("no bytes to write")
	}

	cm := &corev1.ConfigMap{
		BinaryData: map[string][]byte{
			streamObjKey: p,
		},
	}
	// Note: consider making these ConfigMaps content-addressable to avoid creating duplicates.
	cm.SetGenerateName("stream-")
	s.stamp(cm)
	fmt.Printf("cm: %v\n", *cm)

	if err = s.Client.Create(context.TODO(), cm); err != nil {
		return 0, err
	}

	return len(p), nil
}

// Read fills p with up to len(p) content of the next ConfigMap in the stream.
func (s *ConfigMapStream) Read(p []byte) (n int, err error) {
	var elements []corev1.ConfigMap
	if elements, err = s.cache(context.TODO()); err != nil {
		return
	}
	fmt.Printf("|elements|: %d\n", len(elements))
	if s.current >= len(elements) {
		// End of stream, conform to io.Reader behavior (see https://golang.org/pkg/io/#Reader)
		return 0, io.EOF
	}

	var (
		element = s.elements[s.current]
		data    = element.BinaryData[streamObjKey]
		reader  = bytes.NewReader(data)
	)
	n, err = reader.ReadAt(p, s.offset)
	s.offset = s.offset + int64(n)
	if err == io.EOF {
		err = nil
		s.offset = 0
		s.current++
		if n < len(p) {
			// TODO(njhale): make this iterative
			var m int
			m, err = s.Read(p[n-1:])
			n = n + m
		}
	}
	fmt.Printf("current, offset: %d, %d\n", s.current, s.offset)

	return
}

func (s *ConfigMapStream) cache(ctx context.Context) ([]corev1.ConfigMap, error) {
	if len(s.elements) > 0 {
		return s.elements, nil
	}

	var (
		err  error
		list = &corev1.ConfigMapList{}
	)
	err = s.Client.List(ctx, list, client.InNamespace(s.namespace), client.MatchingLabelsSelector{s.labelSelector()})
	if err != nil {
		return nil, err
	}

	s.elements = list.Items
	if len(s.elements) < 1 {
		return nil, fmt.Errorf("no elements of stream found")
	}

	return s.elements, nil
}

func (s *ConfigMapStream) labelSelector() labels.Selector {
	return labels.Set{
		labelKey: s.label,
	}.AsSelector()
}

// stamp applies the stream label and namespace to a resource.
func (s *ConfigMapStream) stamp(obj Object) {
	labels := obj.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}

	labels[labelKey] = s.label
	obj.SetLabels(labels)
	obj.SetNamespace(s.namespace)
}
