package instance_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/instance"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestContextManager(t *testing.T) {
	t.Run("Generate", func(t *testing.T) {
		t.Run("should return compiled instanceSpec config for task type transformation", func(t *testing.T) {

			projectName := "humara-projectSpec"
			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}
			execUnit := new(mock.Transformer)

			jobSpec := models.JobSpec{
				Name:  "foo",
				Owner: "mee@mee",
				Behavior: models.JobSpecBehavior{
					CatchUp:       true,
					DependsOnPast: false,
				},
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2000, 11, 11, 0, 0, 0, 0, time.UTC),
					Interval:  "* * * * *",
				},
				Task: models.JobSpecTask{
					Unit:     execUnit,
					Priority: 2000,
					Window: models.JobSpecTaskWindow{
						Size:       time.Hour,
						Offset:     0,
						TruncateTo: "d",
					},
					Config: models.JobSpecConfigs{
						{
							Name:  "BQ_VAL",
							Value: "22",
						},
						{
							Name:  "EXECT",
							Value: "{{.EXECUTION_TIME}}",
						},
						{
							Name:  "BUCKET",
							Value: "{{.GLOBAL__bucket}}",
						},
						{
							Name:  "LOAD_METHOD",
							Value: "MERGE",
						},
					},
				},
				Dependencies: map[string]models.JobSpecDependency{},
				Assets: *models.JobAssets{}.New(
					[]models.JobSpecAsset{
						{
							Name:  "query.sql",
							Value: "select * from table WHERE event_timestamp > '{{.EXECUTION_TIME}}'",
						},
					},
				),
			}
			mockedTimeNow := time.Now()

			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			instanceSpec := models.InstanceSpec{
				Job:         jobSpec,
				ScheduledAt: scheduledAt,
				State:       models.InstanceStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  instance.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			execUnit.On("Name").Return("bq")
			execUnit.On("CompileAssets", models.CompileAssetsRequest{
				TaskWindow:       jobSpec.Task.Window,
				Config:           jobSpec.Task.Config,
				Assets:           jobSpec.Assets.ToMap(),
				InstanceSchedule: scheduledAt,
				InstanceData:     instanceSpec.Data,
			}).Return(models.CompileAssetsResponse{Assets: map[string]string{
				"query.sql": "select * from table WHERE event_timestamp > '{{.EXECUTION_TIME}}'",
			}}, nil)

			envMap, fileMap, err := instance.NewContextManager(projectSpec, jobSpec,
				instance.NewGoEngine()).Generate(instanceSpec, models.InstanceTypeTransformation, "bq")
			assert.Nil(t, err)

			assert.Equal(t, "2020-11-11T00:00:00Z", envMap["DEND"])
			assert.Equal(t, "2020-11-10T23:00:00Z", envMap["DSTART"])
			assert.Equal(t, mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), envMap["EXECUTION_TIME"])

			assert.Equal(t, "22", envMap["BQ_VAL"])
			assert.Equal(t, mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), envMap["EXECT"])
			assert.Equal(t, projectSpec.Config["bucket"], envMap["BUCKET"])

			assert.Equal(t,
				fmt.Sprintf("select * from table WHERE event_timestamp > '%s'", mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout)),
				fileMap["query.sql"],
			)
		})
		t.Run("should return valid compiled instanceSpec config for task type hook", func(t *testing.T) {

			projectName := "humara-projectSpec"
			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"bucket":                 "gs://some_folder",
					"transporterKafkaBroker": "0.0.0.0:9092",
				},
			}

			execUnit := new(mock.Transformer)

			transporterHook := "transporter"
			hookUnit := new(mock.HookUnit)
			hookUnit.On("Name").Return(transporterHook)

			jobSpec := models.JobSpec{
				Name:  "foo",
				Owner: "mee@mee",
				Behavior: models.JobSpecBehavior{
					CatchUp:       true,
					DependsOnPast: false,
				},
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2000, 11, 11, 0, 0, 0, 0, time.UTC),
					Interval:  "* * * * *",
				},
				Task: models.JobSpecTask{
					Unit:     execUnit,
					Priority: 2000,
					Window: models.JobSpecTaskWindow{
						Size:       time.Hour,
						Offset:     0,
						TruncateTo: "d",
					},
					Config: models.JobSpecConfigs{
						{
							Name:  "BQ_VAL",
							Value: "22",
						},
					},
				},
				Dependencies: map[string]models.JobSpecDependency{},
				Assets: *models.JobAssets{}.New(
					[]models.JobSpecAsset{
						{
							Name:  "query.sql",
							Value: "select * from table WHERE event_timestamp > '{{.EXECUTION_TIME}}'",
						},
					},
				),
				Hooks: []models.JobSpecHook{
					{
						Config: models.JobSpecConfigs{
							{
								Name:  "SAMPLE_CONFIG",
								Value: "200",
							},
							{
								Name:  "INHERIT_CONFIG",
								Value: "{{.TASK__BQ_VAL}}",
							},
							{
								Name:  "FILTER_EXPRESSION",
								Value: "event_timestamp >= '{{.DSTART}}' AND event_timestamp < '{{.DEND}}'",
							},
							{
								Name:  "PRODUCER_CONFIG_BOOTSTRAP_SERVERS",
								Value: `{{.GLOBAL__transporterKafkaBroker}}`,
							},
						},
						Unit: hookUnit,
					},
				},
			}
			mockedTimeNow := time.Now()

			scheduledAt := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
			instanceSpec := models.InstanceSpec{
				Job:         jobSpec,
				ScheduledAt: scheduledAt,
				State:       models.InstanceStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  instance.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  instance.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}
			execUnit.On("Name").Return("bq")
			execUnit.On("CompileAssets", models.CompileAssetsRequest{
				TaskWindow:       jobSpec.Task.Window,
				Config:           jobSpec.Task.Config,
				Assets:           jobSpec.Assets.ToMap(),
				InstanceSchedule: scheduledAt,
				InstanceData:     instanceSpec.Data,
			}).Return(models.CompileAssetsResponse{Assets: map[string]string{
				"query.sql": "select * from table WHERE event_timestamp > '{{.EXECUTION_TIME}}'",
			}}, nil)

			envMap, fileMap, err := instance.NewContextManager(projectSpec, jobSpec, instance.NewGoEngine()).Generate(instanceSpec, models.InstanceTypeHook, transporterHook)
			assert.Nil(t, err)

			assert.Equal(t, "2020-11-11T00:00:00Z", envMap["DEND"])
			assert.Equal(t, "2020-11-10T23:00:00Z", envMap["DSTART"])
			assert.Equal(t, mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), envMap["EXECUTION_TIME"])

			assert.Equal(t, "0.0.0.0:9092", envMap["PRODUCER_CONFIG_BOOTSTRAP_SERVERS"])
			assert.Equal(t, "200", envMap["SAMPLE_CONFIG"])
			assert.Equal(t, "22", envMap["INHERIT_CONFIG"])
			assert.Equal(t, "22", envMap["TASK__BQ_VAL"])

			assert.Equal(t, "event_timestamp >= '2020-11-10T23:00:00Z' AND event_timestamp < '2020-11-11T00:00:00Z'", envMap["FILTER_EXPRESSION"])

			assert.Equal(t,
				fmt.Sprintf("select * from table WHERE event_timestamp > '%s'", mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout)),
				fileMap["query.sql"],
			)
		})
	})
}
