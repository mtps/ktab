package types

import "time"

type MessageHeader struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

type Message struct {
	Offset    int64           `json:"offset"`
	Topic     string          `json:"topic"`
	Key       []byte          `json:"key"`
	Value     []byte          `json:"value"`
	Timestamp time.Time       `json:"timestamp"`
	Partition int32           `json:"partition"`
	Headers   []MessageHeader `json:"headers"`
}
