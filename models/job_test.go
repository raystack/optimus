package models_test

import (
	"testing"
	"time"

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
	t.Run("JobSpecTaskWindow", func(t *testing.T) {
		t.Run("should generate valid window start and end", func(t *testing.T) {
			cases := []struct {
				Today              time.Time
				WindowSize         time.Duration
				WindowOffset       time.Duration
				WindowTruncateUpto string

				ExpectedStart time.Time
				ExpectedEnd   time.Time
			}{
				{
					Today:              time.Date(2021, 2, 25, 0, 0, 0, 0, time.UTC),
					WindowSize:         24 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "",
					ExpectedStart:      time.Date(2021, 2, 24, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 2, 25, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "",
					ExpectedStart:      time.Date(2020, 7, 9, 6, 33, 22, 0, time.UTC),
					ExpectedEnd:        time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
				},
				{
					Today:              time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "h",
					ExpectedStart:      time.Date(2020, 7, 9, 6, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2020, 7, 10, 6, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "d",
					ExpectedStart:      time.Date(2020, 7, 9, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2020, 7, 10, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					WindowSize:         48 * time.Hour,
					WindowOffset:       24 * time.Hour,
					WindowTruncateUpto: "d",
					ExpectedStart:      time.Date(2020, 7, 9, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2020, 7, 11, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * time.Hour,
					WindowOffset:       -24 * time.Hour,
					WindowTruncateUpto: "d",
					ExpectedStart:      time.Date(2020, 7, 8, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2020, 7, 9, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2020, 7, 10, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 7 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "w",
					ExpectedStart:      time.Date(2020, 7, 5, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2020, 7, 12, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2020, 7, 12, 0, 0, 0, 0, time.UTC),
					WindowSize:         24 * 7 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "w",
					ExpectedStart:      time.Date(2020, 7, 12, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2020, 7, 19, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 3, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 3, 1, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 30 * time.Hour,
					WindowOffset:       24 * 32 * time.Hour,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 3, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 3, 31, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 2, 01, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 62 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 30 * time.Hour,
					WindowOffset:       24 * 30 * time.Hour,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 3, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 3, 31, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 30 * time.Hour,
					WindowOffset:       -24 * 30 * time.Hour,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 1, 31, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 20 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 2, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 2, 25, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 60 * time.Hour,
					WindowOffset:       0,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC),
				},
				{
					Today:              time.Date(2021, 3, 31, 6, 33, 22, 0, time.UTC),
					WindowSize:         24 * 30 * time.Hour,
					WindowOffset:       -24 * 30 * time.Hour,
					WindowTruncateUpto: "M",
					ExpectedStart:      time.Date(2021, 2, 1, 0, 0, 0, 0, time.UTC),
					ExpectedEnd:        time.Date(2021, 2, 28, 0, 0, 0, 0, time.UTC),
				},
			}

			for _, tcase := range cases {
				win := &&models.WindowV1{
					SizeAsDuration:   tcase.WindowSize,
					OffsetAsDuration: tcase.WindowOffset,
					TruncateTo:       tcase.WindowTruncateUpto,
				}

				actualStartTime, actualStartError := win.GetStartTime(tcase.Today)
				actualEndTime, actualEndError := win.GetEndTime(tcase.Today)

				assert.Equal(t, tcase.ExpectedStart.String(), actualStartTime.String())
				assert.NoError(t, actualStartError)
				assert.Equal(t, tcase.ExpectedEnd.String(), actualEndTime.String())
				assert.NoError(t, actualEndError)
			}
		})
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
					Window: &&models.WindowV1{
						SizeAsDuration:   time.Hour,
						OffsetAsDuration: 0,
						TruncateTo:       "d",
					},
				},
			}

			slaDefinitionInSec, err := jobSpecs.SLADuration()
			assert.Nil(t, err)

			assert.Equal(t, int64(2), slaDefinitionInSec)
		})
	})
}
