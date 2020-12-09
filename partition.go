package cmstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

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
	fmt.Printf("data %v\n", data)
	fmt.Printf("len(data) %d\n", len(data))
	fmt.Printf("segmentSize: %d\n", p.segmentSize)

	var (
		segment SimpleSegment
		encoder = json.NewEncoder(segments)
	)
	for _, b := range data {
		if len(segment.Data) == p.segmentSize {
			fmt.Printf("writing segment: %b\n", segment.Data)
			if err := encoder.Encode(segment); err != nil {
				return fmt.Errorf("failed to write segment %v to stream: %s", segment, err)
			}

			// Prep next segment
			segment.Position++
			segment.Data = nil
		}
		fmt.Printf("appending fragment: %b\n", b)
		segment.Data = append(segment.Data, b)
	}

	// Encode the final segment if there is any data
	if len(segment.Data) > 0 {
		fmt.Printf("writing final segment: %b\n", segment.Data)
		if err := encoder.Encode(segment); err != nil {
			return fmt.Errorf("failed to write segment %v to stream: %s", segment, err)
		}
	}

	return nil
}

func (p *SimplePartitioner) Join(v interface{}, segments io.Reader) error {
	// TODO(njhale): handle async readers
	var (
		decoder = json.NewDecoder(segments)
		ordered []*SimpleSegment
		err     error
	)
	for {
		fmt.Println("decoding next segment...")
		segment := &SimpleSegment{}
		if err = decoder.Decode(segment); err != nil {
			break
		}
		fmt.Printf("decoded segment: %v\n", segment)

		p := segment.Position
		switch {
		case p == uint(len(ordered)):
			// Rely on append to expand capacity in the case of an in-order stream
			ordered = append(ordered, segment)
		case p > uint(len(ordered)-1):
			// Unordered stream, resize to include the partition
			expanded := make([]*SimpleSegment, segment.Position+1)
			copy(expanded, ordered)
			fallthrough
		default:
			if ordered[p] != nil {
				return fmt.Errorf("received duplicate segment at position %d", p)
			}

			// In-order insert
			ordered[p] = segment
		}

	}
	if err != io.EOF {
		return fmt.Errorf("failed to read segment from stream: %s", err)
	}

	// Collect data and decode to the target interface
	var buf bytes.Buffer
	for _, segment := range ordered {
		if _, err := buf.Write(segment.Data); err != nil {
			return fmt.Errorf("failed to join segments %s", err)
		}
	}

	decoder = json.NewDecoder(&buf)
	if err := decoder.Decode(v); err != nil {
		return fmt.Errorf("failed to decode joined segments: %s", err)
	}

	return nil
}
