package meta

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// KafkaWriter is an abstraction over kafka-go client implementation
type KafkaWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
	Stats() kafka.WriterStats
}

// Writer will be used to write send data to kafka topic
type Writer struct {
	client           KafkaWriter
	bufferSize       int
	bufferedMessages []kafka.Message
}

// NewWriter returns a instance for writer used over kafka client
func NewWriter(w KafkaWriter, buffSize int) *Writer {
	return &Writer{
		client:           w,
		bufferSize:       buffSize,
		bufferedMessages: make([]kafka.Message, 0),
	}
}

// Write push messages to kafka
// this will throw an error if connection was closed in the middle of write
func (w *Writer) Write(protobufkey []byte, protobuf []byte) error {
	msg := kafka.Message{
		Key:   protobufkey,
		Value: protobuf,
	}
	w.bufferedMessages = append(w.bufferedMessages, msg)

	var err error = nil
	if len(w.bufferedMessages) >= w.bufferSize {
		err = w.Flush()
	}
	return err
}

// Flush will push all the queued up messages to kafka
func (w *Writer) Flush() error {
	var err error = nil

	if len(w.bufferedMessages) > 0 {
		err = w.client.WriteMessages(context.Background(), w.bufferedMessages...)
		if err == nil {
			w.bufferedMessages = make([]kafka.Message, 0)
			fmt.Println("Published metadata for ", len(w.bufferedMessages), "specs")
		}
	}
	return err
}
