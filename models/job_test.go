package models_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/models"
)

func TestJob(t *testing.T) {
	t.Run("GetName", func(t *testing.T) {
		jobSpec := models.JobSpec{
			Name: "job-name",
		}
		assert.Equal(t, "job-name", jobSpec.GetName())
	})
	t.Run("GetFullName", func(t *testing.T) {
		jobSpec := models.JobSpec{
			Name: "job-name",
			NamespaceSpec: models.NamespaceSpec{
				Name: "namespace-name",
				ProjectSpec: models.ProjectSpec{
					Name: "project-name",
				},
			},
		}
		assert.Equal(t, "project-name/job-name", jobSpec.GetFullName())
	})
	t.Run("GetJobDependencyMap", func(t *testing.T) {
		t.Run("should able to create a map of job ID and its dependencies", func(t *testing.T) {
			jobID1 := uuid.New()
			jobID2 := uuid.New()
			jobID3 := uuid.New()
			projectSpec := models.ProjectSpec{
				Name: "sample-project",
				Config: map[string]string{
					"bucket": "gs://sample_directory",
				},
			}
			pairs := []models.JobIDDependenciesPair{
				{
					JobID:            jobID1,
					DependentProject: projectSpec,
					DependentJobID:   jobID2,
					Type:             models.JobSpecDependencyTypeIntra,
				},
				{
					JobID:            jobID1,
					DependentProject: projectSpec,
					DependentJobID:   jobID3,
					Type:             models.JobSpecDependencyTypeIntra,
				},
				{
					JobID:            jobID2,
					DependentProject: projectSpec,
					DependentJobID:   jobID3,
					Type:             models.JobSpecDependencyTypeIntra,
				},
			}
			expectedMap := map[uuid.UUID][]models.JobIDDependenciesPair{
				jobID1: {pairs[0], pairs[1]},
				jobID2: {pairs[2]},
			}
			actual := models.JobIDDependenciesPairs(pairs).GetJobDependencyMap()

			assert.Equal(t, expectedMap, actual)
		})
	})
	t.Run("GetExternalProjectAndDependenciesMap", func(t *testing.T) {
		t.Run("should able to get inter project dependencies", func(t *testing.T) {
			jobID1 := uuid.New()
			jobID2 := uuid.New()
			jobID3 := uuid.New()
			jobID4 := uuid.New()
			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
				Name: "sample-project",
				Config: map[string]string{
					"bucket": "gs://sample_directory",
				},
			}
			projectSpec1 := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
				Name: "sample-project-1",
				Config: map[string]string{
					"bucket": "gs://sample_directory_1",
				},
			}
			projectSpec2 := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
				Name: "sample-project-2",
				Config: map[string]string{
					"bucket": "gs://sample_directory_2",
				},
			}
			pairs := []models.JobIDDependenciesPair{
				{
					JobID:            jobID1,
					DependentProject: projectSpec,
					DependentJobID:   jobID2,
					Type:             models.JobSpecDependencyTypeIntra,
				},
				{
					JobID:            jobID1,
					DependentProject: projectSpec1,
					DependentJobID:   jobID3,
					Type:             models.JobSpecDependencyTypeInter,
				},
				{
					JobID:            jobID1,
					DependentProject: projectSpec2,
					DependentJobID:   jobID4,
					Type:             models.JobSpecDependencyTypeInter,
				},
				{
					JobID:            jobID2,
					DependentProject: projectSpec2,
					DependentJobID:   jobID4,
					Type:             models.JobSpecDependencyTypeInter,
				},
			}
			expectedMap := map[models.ProjectID][]models.JobIDDependenciesPair{
				projectSpec1.ID: {pairs[1]},
				projectSpec2.ID: {pairs[2], pairs[3]},
			}
			actual := models.JobIDDependenciesPairs(pairs).GetExternalProjectAndDependenciesMap()

			assert.Equal(t, expectedMap, actual)
		})
	})
	t.Run("GroupJobsPerNamespace", func(t *testing.T) {
		t.Run("should able to get map of namespace name and the jobs", func(t *testing.T) {
			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
				Name: "sample-project",
				Config: map[string]string{
					"bucket": "gs://sample_directory",
				},
			}
			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.New(),
				Name:        "sample-namespace",
				ProjectSpec: projectSpec,
			}
			namespaceSpec1 := models.NamespaceSpec{
				ID:          uuid.New(),
				Name:        "sample-namespace-2",
				ProjectSpec: projectSpec,
			}
			jobSpec1 := models.JobSpec{
				ID:            uuid.New(),
				NamespaceSpec: namespaceSpec,
			}
			jobSpec2 := models.JobSpec{
				ID:            uuid.New(),
				NamespaceSpec: namespaceSpec1,
			}
			jobSpec3 := models.JobSpec{
				ID:            uuid.New(),
				NamespaceSpec: namespaceSpec1,
			}
			jobSpecs := models.JobSpecs{jobSpec1, jobSpec2, jobSpec3}
			expected := map[string][]models.JobSpec{
				namespaceSpec.Name:  {jobSpec1},
				namespaceSpec1.Name: {jobSpec2, jobSpec3},
			}

			actual := jobSpecs.GroupJobsPerNamespace()

			assert.Equal(t, expected, actual)
		})
	})
	t.Run("SLADuration", func(t *testing.T) {
		t.Run("should able to get defined SLA duration", func(t *testing.T) {
			window, err := models.NewWindow(1, "d", "0", "1h")
			if err != nil {
				panic(err)
			}
			jobSpecs := models.JobSpec{
				Name:  "foo",
				Owner: "mee@mee",
				Behavior: models.JobSpecBehavior{
					Notify: []models.JobSpecNotifier{
						{
							On: models.SLAMissEvent,
							Config: map[string]string{
								"duration": "2s",
							},
							Channels: []string{"scheme://route"},
						},
					},
				},
				Task: models.JobSpecTask{
					Unit:     &models.Plugin{},
					Priority: 2000,
					Window:   window,
				},
			}

			slaDefinitionInSec, err := jobSpecs.SLADuration()
			assert.Nil(t, err)

			assert.Equal(t, int64(2), slaDefinitionInSec)
		})
	})
}
