package compiler_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestJobRunAssetsCompiler(t *testing.T) {
	t.Run("CompileJobRunAssets", func(t *testing.T) {
		ctx := context.Background()
		engine := compiler.NewGoEngine()
		execUnit := new(mock.YamlMod)
		depResMod := new(mock.DependencyResolverMod)
		plugin := &models.Plugin{YamlMod: execUnit, DependencyMod: depResMod}

		execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{Name: "bq"}, nil)

		window, err := models.NewWindow(1, "", "", "")
		if err != nil {
			panic(err)
		}
		jobSpec := models.JobSpec{
			Version: 1,
			Name:    "foo",
			Owner:   "optimus",
			Task: models.JobSpecTask{
				Unit:     plugin,
				Priority: 2000,
				Config: models.JobSpecConfigs{
					{
						Name:  "BQ_VAL",
						Value: "22",
					},
				},
				Window: window,
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

		startTime, err := jobSpec.Task.Window.GetStartTime(jobRun.ScheduledAt)
		if err != nil {
			panic(err)
		}
		endTime, err := jobSpec.Task.Window.GetEndTime(jobRun.ScheduledAt)
		if err != nil {
			panic(err)
		}

		mockedTimeNow := time.Now()
		instanceSpec := models.InstanceSpec{
			Name:   "bq",
			Type:   models.InstanceTypeTask,
			Status: models.RunStateRunning,
			Data: []models.JobRunSpecData{
				{
					Name:  models.ConfigKeyExecutionTime,
					Value: mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout),
					Type:  models.InstanceDataTypeEnv,
				},
				{
					Name:  models.ConfigKeyDstart,
					Value: startTime.Format(models.InstanceScheduledAtTimeLayout),
					Type:  models.InstanceDataTypeEnv,
				},
				{
					Name:  models.ConfigKeyDend,
					Value: endTime.Format(models.InstanceScheduledAtTimeLayout),
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
			pluginRepo := mock.NewPluginRepository(t)
			pluginRepo.On("GetByName", "bq").Return(plugin, errors.New("error"))

			assetsCompiler := compiler.NewJobAssetsCompiler(nil, pluginRepo)
			_, err := assetsCompiler.CompileJobRunAssets(ctx, jobRun.Spec, instanceSpec.Data, jobRun.ScheduledAt, templateContext)

			assert.NotNil(t, err)
			assert.Equal(t, "error", err.Error())
		})
		t.Run("compiles the assets when plugin has no yamlMod", func(t *testing.T) {
			pluginRepo := mock.NewPluginRepository(t)
			pluginRepo.On("GetByName", "bq").Return(&models.Plugin{}, nil)

			assetsCompiler := compiler.NewJobAssetsCompiler(engine, pluginRepo)
			assets, err := assetsCompiler.CompileJobRunAssets(ctx, jobRun.Spec, instanceSpec.Data, jobRun.ScheduledAt, templateContext)

			assert.Nil(t, err)
			expectedQuery := fmt.Sprintf("select * from table WHERE event_timestamp > '%s' and name = '%s'",
				mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), secretContext["table_name"])
			assert.Equal(t, expectedQuery, assets["query.sql"])
		})
		t.Run("compiles the assets successfully", func(t *testing.T) {
			depResMod.On("CompileAssets", context.Background(), models.CompileAssetsRequest{
				Config:       models.PluginConfigs{}.FromJobSpec(jobSpec.Task.Config),
				Assets:       models.PluginAssets{}.FromJobSpec(jobSpec.Assets),
				InstanceData: instanceSpec.Data,
				StartTime:    startTime,
				EndTime:      endTime,
			}).Return(&models.CompileAssetsResponse{Assets: models.PluginAssets{
				models.PluginAsset{
					Name:  "query.sql",
					Value: "select * from table WHERE event_timestamp > '{{.EXECUTION_TIME}}' and name = '{{.secret.table_name}}'",
				},
			}}, nil)
			defer depResMod.AssertExpectations(t)

			pluginRepo := mock.NewPluginRepository(t)
			pluginRepo.On("GetByName", "bq").Return(plugin, nil)

			assetsCompiler := compiler.NewJobAssetsCompiler(engine, pluginRepo)
			assets, err := assetsCompiler.CompileJobRunAssets(ctx, jobRun.Spec, instanceSpec.Data, jobRun.ScheduledAt, templateContext)

			assert.Nil(t, err)
			expectedQuery := fmt.Sprintf("select * from table WHERE event_timestamp > '%s' and name = '%s'",
				mockedTimeNow.Format(models.InstanceScheduledAtTimeLayout), secretContext["table_name"])
			assert.Equal(t, expectedQuery, assets["query.sql"])
		})
	})
}
