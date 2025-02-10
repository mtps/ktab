package types

import "github.com/IBM/sarama"

func KafkaToRecord(message *sarama.ConsumerMessage) Message {
	m := Message{
		Offset:    message.Offset,
		Topic:     message.Topic,
		Key:       message.Key,
		Value:     message.Value,
		Timestamp: message.Timestamp,
		Partition: message.Partition,
		Headers: []MessageHeader{},
	}

	for _, header := range message.Headers {
		m.Headers = append(m.Headers, MessageHeader{Key:header.Key, Value:header.Value})
	}

	return m
}
