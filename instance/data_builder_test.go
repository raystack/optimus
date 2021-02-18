package instance_test

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/instance"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"testing"
	"time"
)

func TestDataBuilder(t *testing.T) {
	t.Run("GetData", func(t *testing.T) {
		t.Run("should return compiled instance config", func(t *testing.T) {

			projectName := "humara-project"
			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"bucket":                 "gs://some_folder",
					"transporterKafkaBroker": "0.0.0.0:9092",
				},
			}

			execUnit := new(mock.ExecutionUnit)
			execUnit.On("GetName").Return("bq")

			transporterHook := "transporter"
			hookUnit := new(mock.HookUnit)
			hookUnit.On("GetName").Return(transporterHook)

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
						Type: models.HookTypePre,
						Config: map[string]string{
							"SAMPLE_CONFIG":                     "200",
							"FILTER_EXPRESSION":                 "event_timestamp >= '{{.DSTART}}' AND event_timestamp < '{{.DEND}}'",
							"PRODUCER_CONFIG_BOOTSTRAP_SERVERS": `{{.transporterKafkaBroker}}`,
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
						Name:  "EXECUTION_TIME",
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  "DSTART",
						Value: jobSpec.Task.Window.GetStart(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  "DEND",
						Value: jobSpec.Task.Window.GetEnd(scheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			envMap, fileMap, err := instance.NewDataBuilder().GetData(projectSpec, jobSpec, instanceSpec, models.InstanceTypeHook, transporterHook)
			fmt.Println(envMap, fileMap, err)
			assert.Nil(t, err)
			assert.Equal(t, "2020-11-11T00:00:00Z", envMap["DEND"])
			assert.Equal(t, "2020-11-10T23:00:00Z", envMap["DSTART"])
			assert.Equal(t, "0.0.0.0:9092", envMap["PRODUCER_CONFIG_BOOTSTRAP_SERVERS"])
			assert.Equal(t, "200", envMap["SAMPLE_CONFIG"])
			assert.Equal(t, mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), envMap["EXECUTION_TIME"])
			assert.Equal(t, "event_timestamp >= '2020-11-10T23:00:00Z' AND event_timestamp < '2020-11-11T00:00:00Z'", envMap["FILTER_EXPRESSION"])
			assert.Equal(t,
				fmt.Sprintf("select * from table WHERE event_timestamp > '%s'", mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout)),
				fileMap["query.sql"],
			)
		})
	})
}
