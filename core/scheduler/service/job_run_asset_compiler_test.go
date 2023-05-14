package service_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/scheduler/service"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/internal/models"
	"github.com/odpf/optimus/sdk/plugin"
	smock "github.com/odpf/optimus/sdk/plugin/mock"
)

func TestJobAssetsCompiler(t *testing.T) {
	ctx := context.Background()
	project, _ := tenant.NewProject("proj1", map[string]string{
		"STORAGE_PATH":   "somePath",
		"SCHEDULER_HOST": "localhost",
	})
	namespace, _ := tenant.NewNamespace("ns1", project.Name(), map[string]string{})
	tnnt, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())
	currentTime := time.Now()
	scheduleTime := currentTime.Add(-time.Hour)
	window, _ := models.NewWindow(2, "d", "1h", "24h")
	job := &scheduler.Job{
		Name:   "jobName",
		Tenant: tnnt,
		Task: &scheduler.Task{
			Name: "taskName",
			Config: map[string]string{
				"configName": "configVale",
			},
		},
		Hooks:  nil,
		Window: window,
		Assets: map[string]string{
			"assetName": "assetVale",
		},
	}
	taskName := job.Task.Name
	startTime, _ := job.Window.GetStartTime(scheduleTime)
	endTime, _ := job.Window.GetEndTime(scheduleTime)
	executedAt := currentTime.Add(time.Hour)
	systemEnvVars := map[string]string{
		"DSTART":          startTime.Format(time.RFC3339),
		"DEND":            endTime.Format(time.RFC3339),
		"EXECUTION_TIME":  executedAt.Format(time.RFC3339),
		"JOB_DESTINATION": job.Destination,
	}

	t.Run("CompileJobRunAssets", func(t *testing.T) {
		t.Run("should error if plugin repo get plugin by name fails", func(t *testing.T) {
			pluginRepo := new(mockPluginRepo)
			pluginRepo.On("GetByName", taskName).Return(nil, fmt.Errorf("error in getting plugin by name"))
			defer pluginRepo.AssertExpectations(t)

			contextForTask := map[string]any{}

			jobRunAssetsCompiler := service.NewJobAssetsCompiler(nil, pluginRepo)
			assets, err := jobRunAssetsCompiler.CompileJobRunAssets(ctx, job, systemEnvVars, scheduleTime, contextForTask)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in getting plugin by name")
			assert.Nil(t, assets)
		})
		t.Run("should give error if window get start fails", func(t *testing.T) {
			pluginRepo := new(mockPluginRepo)
			pluginRepo.On("GetByName", "pluginName").Return(&plugin.Plugin{}, nil)
			defer pluginRepo.AssertExpectations(t)

			window1, _ := models.NewWindow(2, "d2", "1h", "24h")
			job1 := &scheduler.Job{
				Name:   "jobName",
				Tenant: tnnt,
				Task: &scheduler.Task{
					Name: "pluginName",
					Config: map[string]string{
						"configName": "configVale",
					},
				},
				Hooks:  nil,
				Window: window1,
				Assets: map[string]string{
					"assetName": "assetVale",
				},
			}

			jobRunAssetsCompiler := service.NewJobAssetsCompiler(nil, pluginRepo)

			contextForTask := map[string]any{}
			assets, err := jobRunAssetsCompiler.CompileJobRunAssets(ctx, job1, systemEnvVars, scheduleTime, contextForTask)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "error getting start time: error validating truncate_to: invalid option provided, provide one of: [h d w M]")
			assert.Nil(t, assets)
		})
		t.Run("compile should return error when DependencyMod CompileAssets fails", func(t *testing.T) {
			yamlMod := new(smock.YamlMod)
			defer yamlMod.AssertExpectations(t)

			dependencyResolverMod := new(smock.DependencyResolverMod)
			dependencyResolverMod.On("CompileAssets", ctx, mock.Anything).Return(nil, fmt.Errorf("error in dependencyMod compile assets"))
			pluginRepo := new(mockPluginRepo)
			pluginRepo.On("GetByName", taskName).Return(&plugin.Plugin{
				DependencyMod: dependencyResolverMod,
				YamlMod:       yamlMod,
			}, nil)
			defer pluginRepo.AssertExpectations(t)
			jobRunAssetsCompiler := service.NewJobAssetsCompiler(nil, pluginRepo)

			contextForTask := map[string]any{}
			assets, err := jobRunAssetsCompiler.CompileJobRunAssets(ctx, job, systemEnvVars, scheduleTime, contextForTask)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in dependencyMod compile assets")
			assert.Nil(t, assets)
		})
		t.Run("compile", func(t *testing.T) {
			yamlMod := new(smock.YamlMod)
			defer yamlMod.AssertExpectations(t)

			dependencyResolverMod := new(smock.DependencyResolverMod)
			dependencyResolverMod.On("CompileAssets", ctx, mock.Anything).Return(&plugin.CompileAssetsResponse{
				Assets: plugin.Assets{
					plugin.Asset{
						Name:  "assetName",
						Value: "assetValue",
					},
				},
			}, nil)
			defer dependencyResolverMod.AssertExpectations(t)

			pluginRepo := new(mockPluginRepo)
			pluginRepo.On("GetByName", taskName).Return(&plugin.Plugin{
				DependencyMod: dependencyResolverMod,
				YamlMod:       yamlMod,
			}, nil)
			defer pluginRepo.AssertExpectations(t)

			contextForTask := map[string]any{}

			t.Run("return error if compiler.compile fails", func(t *testing.T) {
				filesCompiler := new(mockFilesCompiler)
				filesCompiler.On("Compile", map[string]string{"assetName": "assetValue"}, contextForTask).
					Return(nil, fmt.Errorf("error in compiling"))
				defer filesCompiler.AssertExpectations(t)

				jobRunAssetsCompiler := service.NewJobAssetsCompiler(filesCompiler, pluginRepo)
				assets, err := jobRunAssetsCompiler.CompileJobRunAssets(ctx, job, systemEnvVars, scheduleTime, contextForTask)

				assert.NotNil(t, err)
				assert.EqualError(t, err, "error in compiling")
				assert.Nil(t, assets)
			})
			t.Run("return compiled assets", func(t *testing.T) {
				expectedFileMap := map[string]string{
					"filename": "fileContent",
				}

				filesCompiler := new(mockFilesCompiler)
				filesCompiler.On("Compile", map[string]string{"assetName": "assetValue"}, contextForTask).
					Return(expectedFileMap, nil)
				defer filesCompiler.AssertExpectations(t)

				jobRunAssetsCompiler := service.NewJobAssetsCompiler(filesCompiler, pluginRepo)
				assets, err := jobRunAssetsCompiler.CompileJobRunAssets(ctx, job, systemEnvVars, scheduleTime, contextForTask)

				assert.Nil(t, err)
				assert.Equal(t, expectedFileMap, assets)
			})
		})
	})
}

type mockPluginRepo struct {
	mock.Mock
}

func (m *mockPluginRepo) GetByName(name string) (*plugin.Plugin, error) {
	args := m.Called(name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*plugin.Plugin), args.Error(1)
}

type mockFilesCompiler struct {
	mock.Mock
}

func (m *mockFilesCompiler) Compile(fileMap map[string]string, context map[string]any) (map[string]string, error) {
	args := m.Called(fileMap, context)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}
