package compiler_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
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

			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.Must(uuid.NewRandom()),
				Name:        "namespace-1",
				Config:      map[string]string{},
				ProjectSpec: projectSpec,
			}

			execUnit := new(mock.BasePlugin)
			execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "bq",
			}, nil)
			cliMod := new(mock.CLIMod)

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
					Unit:     &models.Plugin{Base: execUnit, CLIMod: cliMod},
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
							Name:  "BUCKETX",
							Value: "{{.proj.bucket}}",
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

			jobRun := models.JobRun{
				Spec:        jobSpec,
				Trigger:     models.TriggerSchedule,
				Status:      models.RunStateAccepted,
				Instances:   nil,
				ScheduledAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
			}
			instanceSpec := models.InstanceSpec{
				Name:   "bq",
				Type:   models.InstanceTypeTask,
				Status: models.RunStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  models.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
				},
				ExecutedAt: time.Time{},
				UpdatedAt:  time.Time{},
			}

			cliMod.On("CompileAssets", context.TODO(), models.CompileAssetsRequest{
				Window:           jobSpec.Task.Window,
				Config:           models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
				Assets:           models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
				InstanceSchedule: jobRun.ScheduledAt,
				InstanceData:     instanceSpec.Data,
			}).Return(&models.CompileAssetsResponse{Assets: models.PluginAssets{
				models.PluginAsset{
					Name:  "query.sql",
					Value: "select * from table WHERE event_timestamp > '{{.EXECUTION_TIME}}'",
				},
			}}, nil)

			jobRunInput, err := compiler.NewContextManager(namespaceSpec, nil, jobRun,
				compiler.NewGoEngine()).Generate(instanceSpec)
			assert.Nil(t, err)

			assert.Equal(t, "2020-11-11T00:00:00Z", jobRunInput.ConfigMap["DEND"])
			assert.Equal(t, "2020-11-10T23:00:00Z", jobRunInput.ConfigMap["DSTART"])
			assert.Equal(t, mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), jobRunInput.ConfigMap["EXECUTION_TIME"])

			assert.Equal(t, "22", jobRunInput.ConfigMap["BQ_VAL"])
			assert.Equal(t, mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), jobRunInput.ConfigMap["EXECT"])
			assert.Equal(t, projectSpec.Config["bucket"], jobRunInput.ConfigMap["BUCKET"])
			assert.Equal(t, projectSpec.Config["bucket"], jobRunInput.ConfigMap["BUCKETX"])

			assert.Equal(t,
				fmt.Sprintf("select * from table WHERE event_timestamp > '%s'", mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout)),
				jobRunInput.FileMap["query.sql"],
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

			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.Must(uuid.NewRandom()),
				Name:        "namespace-1",
				Config:      map[string]string{},
				ProjectSpec: projectSpec,
			}

			execUnit := new(mock.BasePlugin)
			execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "bq",
			}, nil)
			cliMod := new(mock.CLIMod)

			transporterHook := "transporter"
			hookUnit := new(mock.BasePlugin)
			hookUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:       transporterHook,
				PluginType: models.PluginTypeHook,
			}, nil)

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
					Unit:     &models.Plugin{Base: execUnit, CLIMod: cliMod},
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
								Name:  "INHERIT_CONFIG_AS_WELL",
								Value: "{{.task.BQ_VAL}}",
							},
							{
								Name:  "UNKNOWN",
								Value: "{{.task.TT}}",
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
						Unit: &models.Plugin{Base: hookUnit},
					},
				},
			}
			mockedTimeNow := time.Now()

			jobRun := models.JobRun{
				Spec:        jobSpec,
				Trigger:     models.TriggerSchedule,
				Status:      models.RunStateAccepted,
				Instances:   nil,
				ScheduledAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
			}
			instanceSpec := models.InstanceSpec{
				Name:   transporterHook,
				Type:   models.InstanceTypeHook,
				Status: models.RunStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  models.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}
			cliMod.On("CompileAssets", context.TODO(), models.CompileAssetsRequest{
				Window:           jobSpec.Task.Window,
				Config:           models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
				Assets:           models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
				InstanceSchedule: jobRun.ScheduledAt,
				InstanceData:     instanceSpec.Data,
			}).Return(&models.CompileAssetsResponse{Assets: models.PluginAssets{
				models.PluginAsset{
					Name:  "query.sql",
					Value: "select * from table WHERE event_timestamp > '{{.EXECUTION_TIME}}'",
				},
			}}, nil)

			jobRunInput, err := compiler.NewContextManager(namespaceSpec, nil, jobRun, compiler.NewGoEngine()).
				Generate(instanceSpec)
			assert.Nil(t, err)

			assert.Equal(t, "2020-11-11T00:00:00Z", jobRunInput.ConfigMap["DEND"])
			assert.Equal(t, "2020-11-10T23:00:00Z", jobRunInput.ConfigMap["DSTART"])
			assert.Equal(t, mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), jobRunInput.ConfigMap["EXECUTION_TIME"])

			assert.Equal(t, "0.0.0.0:9092", jobRunInput.ConfigMap["PRODUCER_CONFIG_BOOTSTRAP_SERVERS"])
			assert.Equal(t, "200", jobRunInput.ConfigMap["SAMPLE_CONFIG"])
			assert.Equal(t, "22", jobRunInput.ConfigMap["INHERIT_CONFIG"])
			assert.Equal(t, "22", jobRunInput.ConfigMap["INHERIT_CONFIG_AS_WELL"])
			assert.Equal(t, "<no value>", jobRunInput.ConfigMap["UNKNOWN"])

			assert.Equal(t, "event_timestamp >= '2020-11-10T23:00:00Z' AND event_timestamp < '2020-11-11T00:00:00Z'", jobRunInput.ConfigMap["FILTER_EXPRESSION"])

			assert.Equal(t,
				fmt.Sprintf("select * from table WHERE event_timestamp > '%s'", mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout)),
				jobRunInput.FileMap["query.sql"],
			)
		})
		t.Run("should return compiled instanceSpec config with overridden config provided in NamespaceSpec", func(t *testing.T) {
			projectName := "humara-projectSpec"
			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"bucket":              "gs://some_folder",
					"transporter_brokers": "129.3.34.1:9092",
				},
			}
			namespaceSpec := models.NamespaceSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"transporter_brokers": "129.3.34.1:9092-overridden",
				},
				ProjectSpec: projectSpec,
			}

			execUnit := new(mock.BasePlugin)
			cliMod := new(mock.CLIMod)

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
					Unit:     &models.Plugin{Base: execUnit, CLIMod: cliMod},
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
							Name:  "TRANSPORTER_BROKERS",
							Value: "{{.GLOBAL__transporter_brokers}}",
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

			jobRun := models.JobRun{
				Spec:        jobSpec,
				Trigger:     models.TriggerSchedule,
				Status:      models.RunStateAccepted,
				Instances:   nil,
				ScheduledAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
			}
			instanceSpec := models.InstanceSpec{
				Name:   "bq",
				Type:   models.InstanceTypeTask,
				Status: models.RunStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  models.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
				},
			}

			cliMod.On("CompileAssets", context.TODO(), models.CompileAssetsRequest{
				Window:           jobSpec.Task.Window,
				Config:           models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
				Assets:           models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
				InstanceSchedule: jobRun.ScheduledAt,
				InstanceData:     instanceSpec.Data,
			}).Return(&models.CompileAssetsResponse{Assets: models.PluginAssets{
				models.PluginAsset{
					Name:  "query.sql",
					Value: "select * from table WHERE event_timestamp > '{{.EXECUTION_TIME}}'",
				},
			}}, nil)

			assets, err := compiler.NewContextManager(namespaceSpec, nil, jobRun, compiler.NewGoEngine()).
				Generate(instanceSpec)
			assert.Nil(t, err)

			assert.Equal(t, "2020-11-11T00:00:00Z", assets.ConfigMap["DEND"])
			assert.Equal(t, "2020-11-10T23:00:00Z", assets.ConfigMap["DSTART"])
			assert.Equal(t, mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), assets.ConfigMap["EXECUTION_TIME"])

			assert.Equal(t, "22", assets.ConfigMap["BQ_VAL"])
			assert.Equal(t, mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), assets.ConfigMap["EXECT"])
			assert.Equal(t, projectSpec.Config["bucket"], assets.ConfigMap["BUCKET"])
			assert.Equal(t, namespaceSpec.Config["transporter_brokers"], assets.ConfigMap["TRANSPORTER_BROKERS"])

			assert.Equal(t,
				fmt.Sprintf("select * from table WHERE event_timestamp > '%s'", mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout)),
				assets.FileMap["query.sql"],
			)
		})
		t.Run("returns compiled instance spec with secrets", func(t *testing.T) {
			secrets := []models.ProjectSecretItem{
				{
					ID:    uuid.New(),
					Name:  "table_name",
					Value: "secret_table",
					Type:  models.SecretTypeUserDefined,
				},
				{
					ID:    uuid.New(),
					Name:  "bucket",
					Value: "gs://some_secret_bucket",
					Type:  models.SecretTypeUserDefined,
				},
			}
			projectName := "humara-projectSpec"
			projectSpec := models.ProjectSpec{
				ID:   uuid.Must(uuid.NewRandom()),
				Name: projectName,
				Config: map[string]string{
					"bucket": "gs://some_folder",
				},
			}

			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.Must(uuid.NewRandom()),
				Name:        "namespace-1",
				Config:      map[string]string{},
				ProjectSpec: projectSpec,
			}

			execUnit := new(mock.BasePlugin)
			execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "bq",
			}, nil)
			cliMod := new(mock.CLIMod)

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
					Unit:     &models.Plugin{Base: execUnit, CLIMod: cliMod},
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
							Value: "{{.secret.bucket}}",
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
							Value: "select * from table WHERE event_timestamp > '{{.EXECUTION_TIME}}' and name = '{{.secret.table_name}}'",
						},
					},
				),
			}
			mockedTimeNow := time.Now()

			jobRun := models.JobRun{
				Spec:        jobSpec,
				Trigger:     models.TriggerSchedule,
				Status:      models.RunStateAccepted,
				Instances:   nil,
				ScheduledAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
			}
			instanceSpec := models.InstanceSpec{
				Name:   "bq",
				Type:   models.InstanceTypeTask,
				Status: models.RunStateRunning,
				Data: []models.InstanceSpecData{
					{
						Name:  models.ConfigKeyExecutionTime,
						Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDstart,
						Value: jobSpec.Task.Window.GetStart(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
					{
						Name:  models.ConfigKeyDend,
						Value: jobSpec.Task.Window.GetEnd(jobRun.ScheduledAt).Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
				},
				ExecutedAt: time.Time{},
				UpdatedAt:  time.Time{},
			}

			cliMod.On("CompileAssets", context.TODO(), models.CompileAssetsRequest{
				Window:           jobSpec.Task.Window,
				Config:           models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
				Assets:           models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
				InstanceSchedule: jobRun.ScheduledAt,
				InstanceData:     instanceSpec.Data,
			}).Return(&models.CompileAssetsResponse{Assets: models.PluginAssets{
				models.PluginAsset{
					Name:  "query.sql",
					Value: "select * from table WHERE event_timestamp > '{{.EXECUTION_TIME}}' and name = '{{.secret.table_name}}'",
				},
			}}, nil)

			assets, err := compiler.NewContextManager(namespaceSpec, secrets, jobRun,
				compiler.NewGoEngine()).Generate(instanceSpec)
			assert.Nil(t, err)

			assert.Equal(t, "2020-11-11T00:00:00Z", assets.ConfigMap["DEND"])
			assert.Equal(t, "2020-11-10T23:00:00Z", assets.ConfigMap["DSTART"])
			assert.Equal(t, mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), assets.ConfigMap["EXECUTION_TIME"])

			assert.Equal(t, "22", assets.ConfigMap["BQ_VAL"])
			assert.Equal(t, mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), assets.ConfigMap["EXECT"])
			_, ok := assets.ConfigMap["BUCKET"]
			assert.Equal(t, false, ok)
			assert.Equal(t, "gs://some_secret_bucket", assets.SecretsMap["BUCKET"])

			assert.Equal(t,
				fmt.Sprintf("select * from table WHERE event_timestamp > '%s' and name = '%s'",
					mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), secrets[0].Value),
				assets.FileMap["query.sql"],
			)
		})
	})
}
