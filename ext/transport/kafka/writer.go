package kafka

import (
	"context"
	"time"

	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
)

const (
	writeTimeout = time.Second * 3
)

var kafkaQueueCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "publisher_kafka_events_queued_counter",
	Help: "Number of events queued to be published to kafka topic",
})

type Writer struct {
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

	return &Writer{kafkaWriter: writer}
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

	err := w.kafkaWriter.WriteMessages(context.Background(), kafkaMessages...)
	if err == nil {
		kafkaQueueCounter.Add(float64(len(messages)))
		return nil
	}
	return err
}
