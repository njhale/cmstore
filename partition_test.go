package cmstore

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
)

type roundTripTC struct {
	r    *rand.Rand
	data []byte
}

func (roundTripTC) Generate(r *rand.Rand, max int) reflect.Value {
	tc := roundTripTC{
		r:    r,
		data: make([]byte, r.Intn(max+1)), // Prevent a panic from a zero argument being passed in
	}
	if _, err := rand.Read(tc.data); err != nil {
		panic(fmt.Errorf("error generating random data for test case: %s", err))
	}

	return reflect.ValueOf(tc)
}

func testRoundTrip(t *testing.T, partitioner Partitioner) {
	roundTrip := func(c roundTripTC) bool {
		var split bytes.Buffer
		if err := partitioner.Split(c.data, &split); err != nil {
			t.Errorf("failed to split data: %s\n", err)
			return false
		}

		// TODO(njhale): Shuffle segments, careful to not corrupt a segment, to make sure we can handle unordered input
		// shuffled := split.Bytes()
		// c.r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		// split = *bytes.NewBuffer(shuffled)

		var joined []byte
		if err := partitioner.Join(&joined, &split); err != nil {
			t.Errorf("failed to join data: %s\n", err)
			return false
		}

		if !bytes.Equal(joined, c.data) {
			t.Errorf("joined data %s does not match expected %s\n", joined, c.data)
			return false
		}

		return true
	}

	if err := quick.Check(roundTrip, &quick.Config{MaxCount: 100}); err != nil {
		t.Error(err)
	}
}

type simplePartitionerTC struct {
	partitioner *SimplePartitioner
}

func (simplePartitionerTC) Generate(r *rand.Rand, max int) reflect.Value {
	tc := simplePartitionerTC{
		partitioner: &SimplePartitioner{
			segmentSize: r.Intn(max+1) + 1, // Prevent panic from zero argument and ensure the min segmentSize is 1
		},
	}

	return reflect.ValueOf(tc)
}

func testPartitionerRoundTrip(t *testing.T, partitioner Partitioner) {
	roundTrip := func(l uint32) bool {
		data := make([]byte, l)
		if _, err := rand.Read(data); err != nil {
			t.Errorf("failed to read data: %s\n", err)
			return false
		}

		var split bytes.Buffer
		if err := partitioner.Split(data, &split); err != nil {
			t.Errorf("failed to split data: %s\n", err)
			return false
		}

		var joined []byte
		if err := partitioner.Join(&joined, &split); err != nil {
			t.Errorf("failed to join data: %s\n", err)
			return false
		}

		return bytes.Equal(joined, data)

	}

	if err := quick.Check(roundTrip, &quick.Config{MaxCount: 25}); err != nil {
		t.Error(err)
	}

}

func testStringRoundTrip(t *testing.T, partitioner Partitioner) {
	data := []byte("Hello, world!")

	var split bytes.Buffer
	if err := partitioner.Split(data, &split); err != nil {
		t.Errorf("failed to split data: %s\n", err)
		return
	}

	var joined []byte
	if err := partitioner.Join(&joined, &split); err != nil {
		t.Errorf("failed to join data: %s\n", err)
		return
	}

	if !bytes.Equal(joined, data) {
		t.Errorf("joined data %s does not match expected %s\n", joined, data)
	}

	return
}

func TestSimplePartitioner(t *testing.T) {
	quick.Check(func(c simplePartitionerTC) bool {
		testRoundTrip(t, c.partitioner)
		return true
	}, &quick.Config{MaxCount: 16})
	// testPartitionerRoundTrip(t, &SimplePartitioner{segmentSize: 512})
	// testStringRoundTrip(t, &SimplePartitioner{segmentSize: 3})
}
