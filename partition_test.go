package cmstore

import (
	"bytes"
	"math/rand"
	"testing"
	"testing/quick"
)

func testPartitionerRoundTrip(t *testing.T, partitioner Partitioner) {
	roundTrip := func(l uint32) bool {
		data := make([]byte, 0, l)
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

func TestSimplePartitioner(t *testing.T) {
	testPartitionerRoundTrip(t, &SimplePartitioner{segmentSize: 1024})
}
