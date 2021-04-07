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
	writer     models.MetadataWriter
	jobAdapter models.JobMetadataAdapter
}

func NewService(writer models.MetadataWriter, builder models.JobMetadataAdapter) *Service {
	return &Service{
		writer:     writer,
		jobAdapter: builder,
	}
}

func (service Service) Publish(proj models.ProjectSpec, jobSpecs []models.JobSpec, po progress.Observer) error {
	for _, jobSpec := range jobSpecs {
		resource, err := service.jobAdapter.FromJobSpec(proj, jobSpec)
		if err != nil {
			return err
		}

		protoKey, err := service.jobAdapter.CompileKey(resource.Urn)
		if err != nil {
			return errors.Wrapf(err, "failed to compile metadata proto key: %s", resource.Urn)
		}

		protoMsg, err := service.jobAdapter.CompileMessage(resource)
		if err != nil {
			return errors.Wrapf(err, "failed to compile metadata proto message: %s", resource.Urn)
		}

		if err = service.writer.Write(protoKey, protoMsg); err != nil {
			return errors.Wrapf(err, "failed to write metadata message: %s", resource.Urn)
		}
	}

	return nil
}
