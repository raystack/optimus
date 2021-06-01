package meta_test

import (
	"context"
	"testing"

	"github.com/odpf/optimus/meta"
	"github.com/odpf/optimus/mock"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestWriter(t *testing.T) {
	t.Run("should be able to write messages with buffer size greater then 0", func(t *testing.T) {
		key, msg := []byte("somekey"), []byte("somemessage")
		messages := []kafka.Message{
			{
				Key:   key,
				Value: msg,
			},
			{
				Key:   key,
				Value: msg,
			},
			{
				Key:   key,
				Value: msg,
			},
		}

		kafkaWriter := &mock.MetaKafkaWriter{}
		kafkaWriter.On("WriteMessages", context.Background(), messages).Return(nil)
		defer kafkaWriter.AssertExpectations(t)

		writer := meta.NewWriter(kafkaWriter, 3)

		var err error
		for i := 0; i < 3; i++ {
			err = writer.Write(key, msg)
			assert.Nil(t, err)
		}
	})
	t.Run("should be able to write messages with 0 size buffer", func(t *testing.T) {
		key, msg := []byte("somekey"), []byte("somemessage")
		messages := []kafka.Message{
			{
				Key:   key,
				Value: msg,
			},
		}

		kafkaWriter := &mock.MetaKafkaWriter{}
		kafkaWriter.On("WriteMessages", context.Background(), messages).Return(nil)
		defer kafkaWriter.AssertExpectations(t)

		writer := meta.NewWriter(kafkaWriter, 0)

		var err error
		err = writer.Write(key, msg)
		assert.Nil(t, err)
	})
}
