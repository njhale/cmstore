package cmstore

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type data [][]byte

func (d data) Generate(r *rand.Rand, size int) reflect.Value {
	var segments data
	numSegments, segmentSize := r.Intn(size-1)+1, r.Intn(size-1)+1
	for ; numSegments >= 0; numSegments-- {
		segment := make([]byte, segmentSize)
		if _, err := rand.Read(segment); err != nil {
			panic(fmt.Errorf("failed to generate random data for test: %s", err))
		}
		segments = append(segments, segment)
	}

	return reflect.ValueOf(segments)
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
		for _, segment := range d {
			n, err := stream.Write(segment)
			if err != nil {
				t.Errorf("failed to write data: %s", err)
				return false
			}
			if n != len(segment) {
				t.Errorf("failed to write %d given bytes, only %d written", len(d), n)
				return false
			}
			in = append(in, segment...)
		}

		var (
			out []byte
			err error
			n   int
		)
		for err == nil {
			// Read a segment of arbitrary length
			segment := make([]byte, 200)
			n, err = stream.Read(segment)
			if n > 0 {
				out = append(out, segment[:n]...)
			}
			if err != nil {
				continue
			}
		}
		if err != io.EOF {
			t.Errorf("failed to read data: %s", err)
			return false
		}
		fmt.Printf("out: %s\n", out)
		fmt.Printf("in: %s\n", in)
		if !bytes.Equal(out, in) {
			t.Errorf("output %b doesn't match input %b", out, in)
			return false
		}

		return true
	}

	if err := quick.Check(roundTrip, &quick.Config{MaxCount: 100}); err != nil {
		t.Error(err)
	}
}
