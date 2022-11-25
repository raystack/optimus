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
	"github.com/odpf/optimus/models"
)

func TestExecutorCompiler(t *testing.T) {
	ctx := context.Background()

	project, _ := tenant.NewProject("proj1", map[string]string{
		"STORAGE_PATH":   "somePath",
		"SCHEDULER_HOST": "localhost",
	})
	namespace, _ := tenant.NewNamespace("ns1", project.Name(), map[string]string{})
	tenantDetails, _ := tenant.NewTenantDetails(project, namespace)
	tnnt, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())

	currentTime := time.Now()
	scheduleTime := currentTime.Add(-time.Hour)

	t.Run("Compile", func(t *testing.T) {
		t.Run("should give error if tenant service getDetails fails", func(t *testing.T) {
			job := scheduler.Job{
				Name:   "job1",
				Tenant: tnnt,
			}
			config := scheduler.RunConfig{
				Executor: scheduler.Executor{
					Name: "transformer",
					Type: "bq2bq",
				},
				ScheduledAt: scheduleTime,
				JobRunID:    scheduler.JobRunID{},
			}

			tenantService := new(mockTenantService)
			tenantService.On("GetDetails", ctx, tnnt).Return(nil, fmt.Errorf("get details error"))
			defer tenantService.AssertExpectations(t)

			inputCompiler := service.NewJobInputCompiler(tenantService, nil, nil)
			inputExecutor, err := inputCompiler.Compile(ctx, &job, config, currentTime.Add(time.Hour))

			assert.NotNil(t, err)
			assert.EqualError(t, err, "get details error")
			assert.Nil(t, inputExecutor)
		})
		t.Run("should give error if tenant service GetSecrets fails", func(t *testing.T) {
			job := scheduler.Job{
				Name:   "job1",
				Tenant: tnnt,
			}
			config := scheduler.RunConfig{
				Executor: scheduler.Executor{
					Name: "transformer",
					Type: "bq2bq",
				},
				ScheduledAt: currentTime.Add(-time.Hour),
				JobRunID:    scheduler.JobRunID{},
			}

			tenantService := new(mockTenantService)
			tenantService.On("GetDetails", ctx, tnnt).Return(tenantDetails, nil)
			tenantService.On("GetSecrets", ctx, project.Name(), namespace.Name().String()).Return(nil, fmt.Errorf("get secrets error"))
			defer tenantService.AssertExpectations(t)

			inputCompiler := service.NewJobInputCompiler(tenantService, nil, nil)
			inputExecutor, err := inputCompiler.Compile(ctx, &job, config, currentTime.Add(time.Hour))

			assert.NotNil(t, err)
			assert.EqualError(t, err, "get secrets error")
			assert.Nil(t, inputExecutor)
		})
		t.Run("should give error if getSystemDefinedConfigs fails", func(t *testing.T) {
			window1, _ := models.NewWindow(1, "d", "2", "2")
			job := scheduler.Job{
				Name:   "job1",
				Tenant: tnnt,
				Window: window1,
			}
			config := scheduler.RunConfig{
				Executor: scheduler.Executor{
					Name: "transformer",
					Type: "bq2bq",
				},
				ScheduledAt: currentTime.Add(-time.Hour),
				JobRunID:    scheduler.JobRunID{},
			}

			tenantService := new(mockTenantService)
			tenantService.On("GetDetails", ctx, tnnt).Return(tenantDetails, nil)
			tenantService.On("GetSecrets", ctx, project.Name(), namespace.Name().String()).Return([]*tenant.PlainTextSecret{}, nil)
			defer tenantService.AssertExpectations(t)

			inputCompiler := service.NewJobInputCompiler(tenantService, nil, nil)
			inputExecutor, err := inputCompiler.Compile(ctx, &job, config, currentTime.Add(time.Hour))

			assert.NotNil(t, err)
			assert.EqualError(t, err, "failed to parse task window with size 2: time: missing unit in duration \"2\"")
			assert.Nil(t, inputExecutor)
		})
		t.Run("should give error if CompileJobRunAssets fails", func(t *testing.T) {
			window, _ := models.NewWindow(2, "d", "1h", "24h")
			job := scheduler.Job{
				Name:   "job1",
				Tenant: tnnt,
				Window: window,
				Assets: nil,
			}
			config := scheduler.RunConfig{
				Executor: scheduler.Executor{
					Name: "transformer",
					Type: "bq2bq",
				},
				ScheduledAt: currentTime.Add(-time.Hour),
				JobRunID:    scheduler.JobRunID{},
			}

			secret1, _ := tenant.NewPlainTextSecret("secretName", "secretValue")
			secret2, _ := tenant.NewPlainTextSecret("secret2Name", "secret2Value")
			secretsArray := []*tenant.PlainTextSecret{secret1, secret2}

			tenantService := new(mockTenantService)
			tenantService.On("GetDetails", ctx, tnnt).Return(tenantDetails, nil)
			tenantService.On("GetSecrets", ctx, project.Name(), namespace.Name().String()).Return(secretsArray, nil)
			defer tenantService.AssertExpectations(t)

			startTime, _ := job.Window.GetStartTime(config.ScheduledAt)
			endTime, _ := job.Window.GetEndTime(config.ScheduledAt)
			executedAt := currentTime.Add(time.Hour)
			systemDefinedVars := map[string]string{
				"DSTART":          startTime.Format(time.RFC3339),
				"DEND":            endTime.Format(time.RFC3339),
				"EXECUTION_TIME":  executedAt.Format(time.RFC3339),
				"JOB_DESTINATION": job.Destination,
			}
			taskContext := mock.Anything

			assetCompiler := new(mockAssetCompiler)
			assetCompiler.On("CompileJobRunAssets", ctx, &job, systemDefinedVars, scheduleTime, taskContext).Return(nil, fmt.Errorf("CompileJobRunAssets error"))
			defer assetCompiler.AssertExpectations(t)

			inputCompiler := service.NewJobInputCompiler(tenantService, nil, assetCompiler)
			inputExecutor, err := inputCompiler.Compile(ctx, &job, config, executedAt)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "CompileJobRunAssets error")
			assert.Nil(t, inputExecutor)
		})
		t.Run("compileConfigs for Executor type Task ", func(t *testing.T) {
			window, _ := models.NewWindow(2, "d", "1h", "24h")
			job := scheduler.Job{
				Name:        "job1",
				Tenant:      tnnt,
				Destination: "some_destination_table_name",
				Task: &scheduler.Task{
					Name: "bq2bq",
					Config: map[string]string{
						"secret.config": "a.secret.val",
						"some.config":   "val",
					},
				},
				Hooks:  nil,
				Window: window,
				Assets: nil,
			}
			config := scheduler.RunConfig{
				Executor: scheduler.Executor{
					Name: "bq2bq",
					Type: scheduler.ExecutorTask,
				},
				ScheduledAt: currentTime.Add(-time.Hour),
				JobRunID:    scheduler.JobRunID{},
			}

			secret1, _ := tenant.NewPlainTextSecret("secretName", "secretValue")
			secret2, _ := tenant.NewPlainTextSecret("secret2Name", "secret2Value")
			secretsArray := []*tenant.PlainTextSecret{secret1, secret2}

			tenantService := new(mockTenantService)
			tenantService.On("GetDetails", ctx, tnnt).Return(tenantDetails, nil)
			tenantService.On("GetSecrets", ctx, project.Name(), namespace.Name().String()).Return(secretsArray, nil)
			defer tenantService.AssertExpectations(t)

			startTime, _ := job.Window.GetStartTime(config.ScheduledAt)
			endTime, _ := job.Window.GetEndTime(config.ScheduledAt)
			executedAt := currentTime.Add(time.Hour)
			systemDefinedVars := map[string]string{
				"DSTART":          startTime.Format(time.RFC3339),
				"DEND":            endTime.Format(time.RFC3339),
				"EXECUTION_TIME":  executedAt.Format(time.RFC3339),
				"JOB_DESTINATION": job.Destination,
			}
			taskContext := mock.Anything

			compiledFile := map[string]string{
				"someFileName": "fileContents",
			}
			assetCompiler := new(mockAssetCompiler)
			assetCompiler.On("CompileJobRunAssets", ctx, &job, systemDefinedVars, scheduleTime, taskContext).Return(compiledFile, nil)
			defer assetCompiler.AssertExpectations(t)

			t.Run("should give error if compileConfigs conf compilation fails", func(t *testing.T) {
				templateCompiler := new(mockTemplateCompiler)
				templateCompiler.On("Compile", map[string]string{"some.config": "val"}, taskContext).
					Return(nil, fmt.Errorf("some.config compilation error"))
				defer templateCompiler.AssertExpectations(t)

				inputCompiler := service.NewJobInputCompiler(tenantService, templateCompiler, assetCompiler)
				inputExecutor, err := inputCompiler.Compile(ctx, &job, config, executedAt)

				assert.NotNil(t, err)
				assert.EqualError(t, err, "some.config compilation error")
				assert.Nil(t, inputExecutor)
			})
			t.Run("should give error if compileConfigs secret compilation fails", func(t *testing.T) {
				templateCompiler := new(mockTemplateCompiler)
				templateCompiler.On("Compile", map[string]string{"some.config": "val"}, taskContext).
					Return(map[string]string{"some.config.compiled": "val.compiled"}, nil)
				templateCompiler.On("Compile", map[string]string{"secret.config": "a.secret.val"}, taskContext).
					Return(nil, fmt.Errorf("secret.config compilation error"))
				defer templateCompiler.AssertExpectations(t)

				inputCompiler := service.NewJobInputCompiler(tenantService, templateCompiler, assetCompiler)
				inputExecutor, err := inputCompiler.Compile(ctx, &job, config, executedAt)

				assert.NotNil(t, err)
				assert.EqualError(t, err, "secret.config compilation error")
				assert.Nil(t, inputExecutor)
			})
			t.Run("should return successfully and provide expected ExecutorInput", func(t *testing.T) {
				templateCompiler := new(mockTemplateCompiler)
				templateCompiler.On("Compile", map[string]string{"some.config": "val"}, taskContext).
					Return(map[string]string{"some.config.compiled": "val.compiled"}, nil)
				templateCompiler.On("Compile", map[string]string{"secret.config": "a.secret.val"}, taskContext).
					Return(map[string]string{"secret.config.compiled": "a.secret.val.compiled"}, nil)
				defer templateCompiler.AssertExpectations(t)

				inputCompiler := service.NewJobInputCompiler(tenantService, templateCompiler, assetCompiler)
				inputExecutorResp, err := inputCompiler.Compile(ctx, &job, config, executedAt)

				assert.Nil(t, err)
				expectedInputExecutor := &scheduler.ExecutorInput{
					Configs: map[string]string{
						"DSTART":               startTime.Format(time.RFC3339),
						"DEND":                 endTime.Format(time.RFC3339),
						"EXECUTION_TIME":       executedAt.Format(time.RFC3339),
						"JOB_DESTINATION":      job.Destination,
						"some.config.compiled": "val.compiled",
					},
					Secrets: map[string]string{"secret.config.compiled": "a.secret.val.compiled"},
					Files:   compiledFile,
				}
				assert.Equal(t, expectedInputExecutor, inputExecutorResp)
			})
		})
		t.Run("compileConfigs for Executor type Hook", func(t *testing.T) {
			window, _ := models.NewWindow(2, "d", "1h", "24h")
			job := scheduler.Job{
				Name:        "job1",
				Tenant:      tnnt,
				Destination: "some_destination_table_name",
				Task: &scheduler.Task{
					Name: "bq2bq",
					Config: map[string]string{
						"secret.config": "a.secret.val",
						"some.config":   "val",
					},
				},
				Hooks: []*scheduler.Hook{
					{
						Name: "predator",
						Config: map[string]string{
							"hook_secret":      "a.secret.val",
							"hook_some_config": "val",
						},
					},
				},
				Window: window,
				Assets: nil,
			}
			config := scheduler.RunConfig{
				Executor: scheduler.Executor{
					Name: "predator",
					Type: scheduler.ExecutorHook,
				},
				ScheduledAt: currentTime.Add(-time.Hour),
				JobRunID:    scheduler.JobRunID{},
			}

			secret1, _ := tenant.NewPlainTextSecret("secretName", "secretValue")
			secret2, _ := tenant.NewPlainTextSecret("secret2Name", "secret2Value")
			secretsArray := []*tenant.PlainTextSecret{secret1, secret2}

			tenantService := new(mockTenantService)
			tenantService.On("GetDetails", ctx, tnnt).Return(tenantDetails, nil)
			tenantService.On("GetSecrets", ctx, project.Name(), namespace.Name().String()).Return(secretsArray, nil)
			defer tenantService.AssertExpectations(t)

			startTime, _ := job.Window.GetStartTime(config.ScheduledAt)
			endTime, _ := job.Window.GetEndTime(config.ScheduledAt)
			executedAt := currentTime.Add(time.Hour)
			systemDefinedVars := map[string]string{
				"DSTART":          startTime.Format(time.RFC3339),
				"DEND":            endTime.Format(time.RFC3339),
				"EXECUTION_TIME":  executedAt.Format(time.RFC3339),
				"JOB_DESTINATION": job.Destination,
			}
			taskContext := mock.Anything

			compiledFile := map[string]string{
				"someFileName": "fileContents",
			}
			assetCompiler := new(mockAssetCompiler)
			assetCompiler.On("CompileJobRunAssets", ctx, &job, systemDefinedVars, scheduleTime, taskContext).Return(compiledFile, nil)
			defer assetCompiler.AssertExpectations(t)

			templateCompiler := new(mockTemplateCompiler)
			templateCompiler.On("Compile", map[string]string{"some.config": "val"}, taskContext).
				Return(map[string]string{"some.config.compiled": "val.compiled"}, nil)
			templateCompiler.On("Compile", map[string]string{"secret.config": "a.secret.val"}, taskContext).
				Return(map[string]string{"secret.config.compiled": "a.secret.val.compiled"}, nil)
			templateCompiler.On("Compile", map[string]string{"hook_some_config": "val"}, taskContext).
				Return(map[string]string{"hook.compiled": "hook.val.compiled"}, nil)
			templateCompiler.On("Compile", map[string]string{"hook_secret": "a.secret.val"}, taskContext).
				Return(map[string]string{"secret.hook.compiled": "hook.s.val.compiled"}, nil)
			defer templateCompiler.AssertExpectations(t)

			inputCompiler := service.NewJobInputCompiler(tenantService, templateCompiler, assetCompiler)
			inputExecutorResp, err := inputCompiler.Compile(ctx, &job, config, executedAt)

			assert.Nil(t, err)
			expectedInputExecutor := &scheduler.ExecutorInput{
				Configs: map[string]string{
					"DSTART":          startTime.Format(time.RFC3339),
					"DEND":            endTime.Format(time.RFC3339),
					"EXECUTION_TIME":  executedAt.Format(time.RFC3339),
					"JOB_DESTINATION": job.Destination,
					"hook.compiled":   "hook.val.compiled",
				},
				Secrets: map[string]string{"secret.hook.compiled": "hook.s.val.compiled"},
				Files:   compiledFile,
			}
			assert.Equal(t, expectedInputExecutor, inputExecutorResp)
		})
		t.Run("compileConfigs for Executor type Hook should fail if error in hook compilation", func(t *testing.T) {
			window, _ := models.NewWindow(2, "d", "1h", "24h")
			job := scheduler.Job{
				Name:        "job1",
				Tenant:      tnnt,
				Destination: "some_destination_table_name",
				Task: &scheduler.Task{
					Name: "bq2bq",
					Config: map[string]string{
						"secret.config": "a.secret.val",
						"some.config":   "val",
					},
				},
				Hooks: []*scheduler.Hook{
					{
						Name: "predator",
						Config: map[string]string{
							"hook_secret":      "a.secret.val",
							"hook_some_config": "val",
						},
					},
				},
				Window: window,
				Assets: nil,
			}
			config := scheduler.RunConfig{
				Executor: scheduler.Executor{
					Name: "predator",
					Type: scheduler.ExecutorHook,
				},
				ScheduledAt: currentTime.Add(-time.Hour),
				JobRunID:    scheduler.JobRunID{},
			}

			secret1, _ := tenant.NewPlainTextSecret("secretName", "secretValue")
			secret2, _ := tenant.NewPlainTextSecret("secret2Name", "secret2Value")
			secretsArray := []*tenant.PlainTextSecret{secret1, secret2}

			tenantService := new(mockTenantService)
			tenantService.On("GetDetails", ctx, tnnt).Return(tenantDetails, nil)
			tenantService.On("GetSecrets", ctx, project.Name(), namespace.Name().String()).Return(secretsArray, nil)
			defer tenantService.AssertExpectations(t)

			startTime, _ := job.Window.GetStartTime(config.ScheduledAt)
			endTime, _ := job.Window.GetEndTime(config.ScheduledAt)
			executedAt := currentTime.Add(time.Hour)
			systemDefinedVars := map[string]string{
				"DSTART":          startTime.Format(time.RFC3339),
				"DEND":            endTime.Format(time.RFC3339),
				"EXECUTION_TIME":  executedAt.Format(time.RFC3339),
				"JOB_DESTINATION": job.Destination,
			}
			taskContext := mock.Anything

			compiledFile := map[string]string{
				"someFileName": "fileContents",
			}
			assetCompiler := new(mockAssetCompiler)
			assetCompiler.On("CompileJobRunAssets", ctx, &job, systemDefinedVars, scheduleTime, taskContext).Return(compiledFile, nil)
			defer assetCompiler.AssertExpectations(t)

			templateCompiler := new(mockTemplateCompiler)
			templateCompiler.On("Compile", map[string]string{"some.config": "val"}, taskContext).
				Return(map[string]string{"some.config.compiled": "val.compiled"}, nil)
			templateCompiler.On("Compile", map[string]string{"secret.config": "a.secret.val"}, taskContext).
				Return(map[string]string{"secret.config.compiled": "a.secret.val.compiled"}, nil)
			templateCompiler.On("Compile", map[string]string{"hook_some_config": "val"}, taskContext).
				Return(nil, fmt.Errorf("error in compiling hook template"))

			defer templateCompiler.AssertExpectations(t)

			inputCompiler := service.NewJobInputCompiler(tenantService, templateCompiler, assetCompiler)
			inputExecutorResp, err := inputCompiler.Compile(ctx, &job, config, executedAt)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in compiling hook template")
			assert.Nil(t, inputExecutorResp)
		})
		t.Run("compileConfigs for Executor type Hook, should raise error if hooks not there in job", func(t *testing.T) {
			window, _ := models.NewWindow(2, "d", "1h", "24h")
			job := scheduler.Job{
				Name:        "job1",
				Tenant:      tnnt,
				Destination: "some_destination_table_name",
				Task: &scheduler.Task{
					Name: "bq2bq",
					Config: map[string]string{
						"secret.config": "a.secret.val",
						"some.config":   "val",
					},
				},
				Hooks:  nil,
				Window: window,
				Assets: nil,
			}
			config := scheduler.RunConfig{
				Executor: scheduler.Executor{
					Name: "predator",
					Type: scheduler.ExecutorHook,
				},
				ScheduledAt: currentTime.Add(-time.Hour),
				JobRunID:    scheduler.JobRunID{},
			}

			secret1, _ := tenant.NewPlainTextSecret("secretName", "secretValue")
			secret2, _ := tenant.NewPlainTextSecret("secret2Name", "secret2Value")
			secretsArray := []*tenant.PlainTextSecret{secret1, secret2}

			tenantService := new(mockTenantService)
			tenantService.On("GetDetails", ctx, tnnt).Return(tenantDetails, nil)
			tenantService.On("GetSecrets", ctx, project.Name(), namespace.Name().String()).Return(secretsArray, nil)
			defer tenantService.AssertExpectations(t)

			startTime, _ := job.Window.GetStartTime(config.ScheduledAt)
			endTime, _ := job.Window.GetEndTime(config.ScheduledAt)
			executedAt := currentTime.Add(time.Hour)
			systemDefinedVars := map[string]string{
				"DSTART":          startTime.Format(time.RFC3339),
				"DEND":            endTime.Format(time.RFC3339),
				"EXECUTION_TIME":  executedAt.Format(time.RFC3339),
				"JOB_DESTINATION": job.Destination,
			}
			taskContext := mock.Anything

			compiledFile := map[string]string{
				"someFileName": "fileContents",
			}
			assetCompiler := new(mockAssetCompiler)
			assetCompiler.On("CompileJobRunAssets", ctx, &job, systemDefinedVars, scheduleTime, taskContext).Return(compiledFile, nil)
			defer assetCompiler.AssertExpectations(t)

			templateCompiler := new(mockTemplateCompiler)
			templateCompiler.On("Compile", map[string]string{"some.config": "val"}, taskContext).
				Return(map[string]string{"some.config.compiled": "val.compiled"}, nil)
			templateCompiler.On("Compile", map[string]string{"secret.config": "a.secret.val"}, taskContext).
				Return(map[string]string{"secret.config.compiled": "a.secret.val.compiled"}, nil)
			defer templateCompiler.AssertExpectations(t)

			inputCompiler := service.NewJobInputCompiler(tenantService, templateCompiler, assetCompiler)
			inputExecutorResp, err := inputCompiler.Compile(ctx, &job, config, executedAt)

			assert.NotNil(t, err)
			assert.Nil(t, inputExecutorResp)
			assert.EqualError(t, err, "not found for entity jobRun: hook not found in job predator")
		})
	})
}

type mockTenantService struct {
	mock.Mock
}

func (m *mockTenantService) GetDetails(ctx context.Context, tnnt tenant.Tenant) (*tenant.WithDetails, error) {
	args := m.Called(ctx, tnnt)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*tenant.WithDetails), args.Error(1)
}

func (m *mockTenantService) GetSecrets(ctx context.Context, projName tenant.ProjectName, nsName string) ([]*tenant.PlainTextSecret, error) {
	args := m.Called(ctx, projName, nsName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*tenant.PlainTextSecret), args.Error(1)
}

type mockAssetCompiler struct {
	mock.Mock
}

func (m *mockAssetCompiler) CompileJobRunAssets(ctx context.Context, job *scheduler.Job, systemEnvVars map[string]string, scheduledAt time.Time, contextForTask map[string]interface{}) (map[string]string, error) {
	args := m.Called(ctx, job, systemEnvVars, scheduledAt, contextForTask)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}

type mockTemplateCompiler struct {
	mock.Mock
}

func (m *mockTemplateCompiler) Compile(templateMap map[string]string, context map[string]any) (map[string]string, error) {
	args := m.Called(templateMap, context)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]string), args.Error(1)
}
