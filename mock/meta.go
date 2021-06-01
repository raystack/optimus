package mock

import (
	"context"

	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
)

type MetaSvcFactory struct {
	mock.Mock
}

func (srv *MetaSvcFactory) New() models.MetadataService {
	return srv.Called().Get(0).(models.MetadataService)
}

// MetaService responsible for collecting and publishing meta data
type MetaService struct {
	mock.Mock
}

func (srv *MetaService) Publish(namespaceSpec models.NamespaceSpec, jobSpecs []models.JobSpec, po progress.Observer) error {
	return srv.Called(namespaceSpec, jobSpecs, po).Error(0)
}

type MetaWriter struct {
	mock.Mock
}

func (w *MetaWriter) Write(key []byte, msg []byte) error {
	return w.Called(key, msg).Error(0)
}

func (w *MetaWriter) Flush() error {
	return w.Called().Error(0)
}

type MetaKafkaWriter struct {
	mock.Mock
}

func (w *MetaKafkaWriter) WriteMessages(con context.Context, msgs ...kafka.Message) error {
	return w.Called(con, msgs).Error(0)
}

func (w *MetaKafkaWriter) Close() error {
	return w.Called().Error(0)
}

func (w *MetaKafkaWriter) Stats() kafka.WriterStats {
	w.Called()
	return kafka.WriterStats{}
}

// MetaBuilder assembles meta
type MetaBuilder struct {
	mock.Mock
}

func (b *MetaBuilder) FromJobSpec(namespace models.NamespaceSpec, jobSpec models.JobSpec) (*models.JobMetadata, error) {
	args := b.Called(namespace, jobSpec)
	return args.Get(0).(*models.JobMetadata), args.Error(1)
}

func (b *MetaBuilder) CompileMessage(r *models.JobMetadata) ([]byte, error) {
	args := b.Called(r)
	return args.Get(0).([]byte), args.Error(1)
}

func (b *MetaBuilder) CompileKey(s string) ([]byte, error) {
	args := b.Called(s)
	return args.Get(0).([]byte), args.Error(1)
}
