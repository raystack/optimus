package meta

import (
	"github.com/pkg/errors"
	"github.com/odpf/optimus/core/progress"
	"github.com/odpf/optimus/models"
)

type MetaSvcFactory interface {
	New() models.MetadataService
}

type Service struct {
	writer  models.MetadataWriter
	builder models.MetadataBuilder
}

func NewService(writer models.MetadataWriter, builder models.MetadataBuilder) *Service {
	return &Service{
		writer:  writer,
		builder: builder,
	}
}

func (service Service) Publish(jobSpecs []models.JobSpec, po progress.Observer) error {
	for _, jobSpec := range jobSpecs {
		resource, err := service.builder.FromJobSpec(jobSpec)
		if err != nil {
			return err
		}

		protoKey, err := service.builder.CompileKey(jobSpec.Name)
		if err != nil {
			return errors.Wrapf(err, "failed to compile metadata proto key: %s", resource.Urn)
		}

		protoMsg, err := service.builder.CompileMessage(resource)
		if err != nil {
			return errors.Wrapf(err, "failed to compile metadata proto message: %s", resource.Urn)
		}

		if err = service.writer.Write(protoKey, protoMsg); err != nil {
			return errors.Wrapf(err, "failed to write metadata message: %s", resource.Urn)
		}
	}

	return nil
}
