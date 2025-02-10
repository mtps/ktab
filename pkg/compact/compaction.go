package compact

import (
	"github.com/IBM/sarama"
)

func Compact(msgs []*sarama.ConsumerMessage) []*sarama.ConsumerMessage {
	compacted := make([]*sarama.ConsumerMessage, 0)
	msgMap := make(map[string]*sarama.ConsumerMessage)
	// Iterate thru all messages and dedupe based on key (keep latest timestamps).
	for _, msg := range msgs {
		current, exists := msgMap[string(msg.Key)]
		// Dedupe for existing by timestamp.
		if !exists || msg.Timestamp.After(current.Timestamp) {
			msgMap[string(msg.Key)] = msg
		}
	}
	// Copy the messages back into the array.
	for _, v := range msgMap {
		compacted = append(compacted, v)
	}
	return compacted
}
