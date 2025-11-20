package checkpoint

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

type TopicCheckpoint struct {
	Topic     string
	Partition int32
	Offset    int64
}

type Checkpoint struct {
	File             string
	Version          int
	TopicCheckpoints []TopicCheckpoint
}

// file contents:
// $version
// $numEntries
// $topic $partition $lastReadOffset
// $topic $partition $lastReadOffset
// ...
// $topic $partition $lastReadOffset
func (t Checkpoint) Write() error {
	buffer := bytes.Buffer{}
	buffer.WriteString(fmt.Sprintf("%d\n", t.Version))
	buffer.WriteString(fmt.Sprintf("%d\n", len(t.TopicCheckpoints)))
	for _, tc := range t.TopicCheckpoints {
		buffer.WriteString(fmt.Sprintf("%s %d %d\n", tc.Topic, tc.Partition, tc.Offset))
	}

	f, err := os.Create(t.File)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err = f.WriteString(buffer.String()); err != nil {
		return err
	}

	return nil
}

func (t *Checkpoint) PutOffset(topic string, partition int32, offset int64) {
	for idx, tc := range t.TopicCheckpoints {
		if tc.Topic == topic && tc.Partition == partition {
			tc.Offset = offset
			t.TopicCheckpoints[idx] = tc
			return
		}
	}

	t.TopicCheckpoints = append(t.TopicCheckpoints, TopicCheckpoint{
		Topic:     topic,
		Partition: partition,
		Offset:    offset + 1,
	})
}

func (t Checkpoint) GetOffset(topic string, partition int32) int64 {
	for _, tc := range t.TopicCheckpoints {
		if tc.Topic == topic && tc.Partition == partition {
			return tc.Offset
		}
	}
	return -1
}

func LoadCheckpoint(filename string) (t *Checkpoint, err error) {
	t = &Checkpoint{File: filename}

	checkpointBytes, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("No checkpoint file found, using empty")
			err = nil
			return
		}
		return
	}

	lines := strings.Split(string(checkpointBytes), "\n")
	if len(lines) < 2 {
		err = fmt.Errorf("invalid checkpoint file format")
		return
	}

	if t.Version, err = strconv.Atoi(lines[0]); err != nil {
		return
	}

	count, err := strconv.Atoi(lines[1])
	if err != nil {
		return
	}

	for i := 2; i < 2+count; i++ {
		parts := strings.Split(lines[i], " ")
		if len(parts) != 3 {
			err = fmt.Errorf("invalid checkpoint for topic: parity mismatch: line %d: %w", i+1, err)
			return
		}

		var p int64
		p, err = strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			err = fmt.Errorf("invalid checkpoint for topic: partition parse error: line %d: %w", i+1, err)
			return
		}

		var o int64
		o, err = strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			err = fmt.Errorf("invalid checkpoint for topic: offset parse error: line %d: %w", i+1, err)
			return
		}

		t.TopicCheckpoints = append(t.TopicCheckpoints, TopicCheckpoint{
			Topic:     parts[0],
			Partition: int32(p),
			Offset:    o,
		})
	}
	return
}
