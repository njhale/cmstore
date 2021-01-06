package cmstore

import (
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type data [][]byte

func (d data) Generate(r *rand.Rand, size int) reflect.Value {
	var objects data
	numObjects, objectSize := r.Intn(size-1)+1, r.Intn(size-1)+1
	for ; numObjects >= 0; numObjects-- {
		object := make([]byte, objectSize)
		if _, err := rand.Read(object); err != nil {
			panic(fmt.Errorf("failed to generate random data for test: %s", err))
		}
		objects = append(objects, object)
	}

	return reflect.ValueOf(objects)
}

func TestConfigMapStreamRoundTrip(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		panic(fmt.Errorf("failed to add types to fake client scheme: %s", err))
	}

	roundTrip := func(d data) bool {
		client := fake.NewFakeClientWithScheme(scheme)
		stream := &ConfigMapStream{
			Client:    client,
			label:     fmt.Sprintf("streamer-%d", len(d)),
			namespace: "default",
		}
		var in []byte
		for _, object := range d {
			n, err := stream.Write(object)
			if err != nil {
				t.Errorf("failed to write data: %s", err)
				return false
			}
			if n != len(object) {
				t.Errorf("failed to write %d given bytes, only %d written", len(d), n)
				return false
			}
			in = append(in, object...)
		}

		var (
			out []byte
			err error
			n   int
		)
		for err == nil {
			// Read a object of arbitrary length
			object := make([]byte, len(d))
			n, err = stream.Read(object)
			if n > 0 {
				out = append(out, object[:n]...)
			}
			if err != nil {
				continue
			}
		}
		if err != io.EOF {
			t.Errorf("failed to read data: %s", err)
			return false
		}

		if !assert.ElementsMatch(t, out, in) {
			t.Errorf("output %b doesn't match input %b", out, in)
			return false
		}

		return true
	}

	if err := quick.Check(roundTrip, &quick.Config{MaxCount: 100}); err != nil {
		t.Error(err)
	}
}
