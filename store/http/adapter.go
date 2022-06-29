package http

import (
	"github.com/odpf/optimus/models"
)

func toJobSpecs(responses []jobSpecificationResponse) []models.JobSpec {
	output := make([]models.JobSpec, len(responses))
	for i, r := range responses {
		output[i] = toJobSpec(r)
	}
	return output
}

func toJobSpec(response jobSpecificationResponse) models.JobSpec {
	return models.JobSpec{
		Name: response.Job.Name,
		NamespaceSpec: models.NamespaceSpec{
			Name: response.NamespaceName,
			ProjectSpec: models.ProjectSpec{
				Name: response.ProjectName,
			},
		},
	}
}
