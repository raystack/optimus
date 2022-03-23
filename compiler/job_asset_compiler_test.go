package compiler_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestJobRunAssetsCompiler(t *testing.T) {
	t.Run("CompileJobRunAssets", func(t *testing.T) {
		engine := compiler.NewGoEngine()
		execUnit := new(mock.BasePlugin)
		cliMod := new(mock.CLIMod)
		pluginRepo := new(mock.SupportedPluginRepo)
		plugin := &models.Plugin{Base: execUnit, CLIMod: cliMod}

		jobSpec := models.JobSpec{
			Name:  "foo",
			Owner: "optimus",
			Task: models.JobSpecTask{
				Unit:     plugin,
				Priority: 2000,
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
						Value: "select * from table WHERE event_timestamp > '{{.EXECUTION_TIME}}' and name = '{{.secret.table_name}}'",
					},
				},
			),
		}

		jobRun := models.JobRun{
			Spec:        jobSpec,
			Trigger:     models.TriggerSchedule,
			Status:      models.RunStateAccepted,
			Instances:   nil,
			ScheduledAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
		}

		mockedTimeNow := time.Now()
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

		secretContext := map[string]string{
			"table_name": "secret_table",
		}
		templateContext := map[string]interface{}{
			"GLOBAL__bucket": "gs://global_bucket",
			"EXECUTION_TIME": mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
			"secret":         secretContext,
		}

		t.Run("returns error when error while getting plugin", func(t *testing.T) {
			execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "bq",
			}, nil)

			pluginRepo.On("GetByName", "bq").Return(&models.Plugin{}, errors.New("error"))

			assetsCompiler := compiler.NewJobAssetsCompiler(engine, pluginRepo)
			_, err := assetsCompiler.CompileJobRunAssets(jobRun, instanceSpec, templateContext)

			assert.NotNil(t, err)
			assert.Equal(t, "error", err.Error())
		})
		t.Run("compiles the assets when no plugin with name", func(t *testing.T) {
			execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "bq",
			}, nil)

			pluginRepo.On("GetByName", "bq").Return(&models.Plugin{}, nil)

			assetsCompiler := compiler.NewJobAssetsCompiler(engine, pluginRepo)
			assets, err := assetsCompiler.CompileJobRunAssets(jobRun, instanceSpec, templateContext)

			assert.Nil(t, err)
			expectedQuery := fmt.Sprintf("select * from table WHERE event_timestamp > '%s' and name = '%s'",
				mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), secretContext["table_name"])
			assert.Equal(t, expectedQuery, assets["query.sql"])
		})
		t.Run("compiles the assets when no plugin with name", func(t *testing.T) {
			execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name: "bq",
			}, nil)

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
			pluginRepo.On("GetByName", "bq").Return(plugin, nil)

			assetsCompiler := compiler.NewJobAssetsCompiler(engine, pluginRepo)
			assets, err := assetsCompiler.CompileJobRunAssets(jobRun, instanceSpec, templateContext)

			assert.Nil(t, err)
			expectedQuery := fmt.Sprintf("select * from table WHERE event_timestamp > '%s' and name = '%s'",
				mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), secretContext["table_name"])
			assert.Equal(t, expectedQuery, assets["query.sql"])
		})
	})
}
