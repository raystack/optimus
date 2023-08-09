package kafka

import (
	"context"
	"time"

	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"

	"github.com/goto/optimus/internal/errors"
)

const (
	writeTimeout = time.Second * 3
)

var kafkaQueueCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "publisher_kafka_events_queued_total",
	Help: "Number of events queued to be published to kafka topic",
})

type Writer struct {
	logger log.Logger

	kafkaWriter *kafka.Writer
}

func NewWriter(kafkaBrokerUrls []string, topic string, logger log.Logger) *Writer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBrokerUrls...),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireOne,
		MaxAttempts:            1,
		WriteTimeout:           writeTimeout,
		Logger:                 kafka.LoggerFunc(logger.Info),
		ErrorLogger:            kafka.LoggerFunc(logger.Error),
	}

	return &Writer{kafkaWriter: writer, logger: logger}
}

func (w *Writer) Close() error {
	return w.kafkaWriter.Close()
}

func (w *Writer) Write(messages [][]byte) error {
	kafkaMessages := make([]kafka.Message, len(messages))
	for i, m := range messages {
		kafkaMessages[i] = kafka.Message{
			Value: m,
		}
	}

	return w.send(kafkaMessages)
}

func (w *Writer) send(messages []kafka.Message) error {
	err := w.kafkaWriter.WriteMessages(context.Background(), messages...)
	if err != nil {
		var messageSizeError kafka.MessageTooLargeError
		if errors.As(err, &messageSizeError) {
			w.logger.Error("Received too large message error for a message, trying remaining")
			w.logger.Error("Discarded message: %s", string(messageSizeError.Message.Value))

			return w.send(messageSizeError.Remaining)
		}

		return err
	}

	kafkaQueueCounter.Add(float64(len(messages)))
	return nil
}
