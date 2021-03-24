package meta_test

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/meta"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"testing"
)

func TestService(t *testing.T) {
	jobSpecs := []models.JobSpec{
		{
			Name: "job-1",
			Task: models.JobSpecTask{
				Unit: nil,
				Config: models.JobSpecConfigs{
					{
						Name:  "do",
						Value: "this",
					},
				},
			},
			Assets: *models.JobAssets{}.New(
				[]models.JobSpecAsset{
					{
						Name:  "query.sql",
						Value: "select * from 1",
					},
				}),
		},
	}

	t.Run("should publish the job specs metadata", func(t *testing.T) {
		resource := &models.ResourceMetadata{Urn: jobSpecs[0].Name}
		protoKey := []byte("key")
		protoMsg := []byte("message")

		builder := new(mock.MetaBuilder)
		builder.On("FromJobSpec", jobSpecs[0]).Return(resource, nil)
		builder.On("CompileKey", jobSpecs[0].Name).Return(protoKey, nil)
		builder.On("CompileMessage", resource).Return(protoMsg, nil)
		defer builder.AssertExpectations(t)

		writer := new(mock.MetaWriter)
		writer.On("Write", protoKey, protoMsg).Return(nil)
		defer writer.AssertExpectations(t)

		po := new(mock.PipelineLogObserver)
		service := meta.NewService(writer, builder)
		err := service.Publish(jobSpecs, po)

		assert.Nil(t, err)
	})

	t.Run("should return error if writing to kafka fails", func(t *testing.T) {
		resource := &models.ResourceMetadata{Urn: jobSpecs[0].Name}
		protoKey := []byte("key")
		protoMsg := []byte("message")

		builder := new(mock.MetaBuilder)
		builder.On("FromJobSpec", jobSpecs[0]).Return(resource, nil)
		builder.On("CompileKey", jobSpecs[0].Name).Return(protoKey, nil)
		builder.On("CompileMessage", resource).Return(protoMsg, nil)
		defer builder.AssertExpectations(t)

		writerErr := errors.New("kafka is down")
		writer := new(mock.MetaWriter)
		writer.On("Write", protoKey, protoMsg).Return(writerErr)
		defer writer.AssertExpectations(t)

		po := new(mock.PipelineLogObserver)
		service := meta.NewService(writer, builder)
		err := service.Publish(jobSpecs, po)

		assert.NotNil(t, err)
		assert.Equal(t, "failed to write metadata message: job-1: kafka is down", err.Error())
	})
}
