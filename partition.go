package cmstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ConfigMapStream struct {
	client   client.Client
	segments []corev1.ConfigMap
}

func (s *ConfigMapStream) Write(p []byte) (n int, err error) {
	return
}

func (s *ConfigMapStream) Read(p []byte) (n int, err error) {
	return
}

func (s *ConfigMapStream) Flush() error {
	return nil
}

type Partitioner interface {
	Split(v interface{}, segments io.Writer) error
	Join(v interface{}, segments io.Reader) error
}

type SimpleSegment struct {
	Position uint   `json:"position"`
	Data     []byte `json:"data"`
}

type SimplePartitioner struct {
	segmentSize int
}

func (p *SimplePartitioner) Split(v interface{}, segments io.Writer) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to encode v: %s", err)
	}

	var (
		segment SimpleSegment
		encoder = json.NewEncoder(segments)
	)
	for i, b := range data {
		if len(segment.Data) < p.segmentSize {
			segment.Data = append(segment.Data, b)

			// Determine if this is a partial segment
			if i < len(data)-1 {
				// Keep building
				continue
			}
		}

		if err := encoder.Encode(segment); err != nil {
			return fmt.Errorf("failed to write segment %s to stream: %s", segment, err)
		}

		// Prep next segment
		segment.Position++
		segment.Data = nil
	}

	return nil
}

func (p *SimplePartitioner) Join(v interface{}, segments io.Reader) error {
	// TODO(njhale): handle async readers
	var data bytes.Buffer
	for {
		data.ReadFrom
		segments.Read(
	}

	return nil
}
