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

func TestJobAssetsCompiler(t *testing.T) {
	ctx := context.Background()
	project, _ := tenant.NewProject("proj1", map[string]string{
		"STORAGE_PATH":   "somePath",
		"SCHEDULER_HOST": "localhost",
	})
	namespace, _ := tenant.NewNamespace("ns1", project.Name(), map[string]string{})
	//tenantDetails, _ := tenant.NewTenantDetails(project, namespace)
	tnnt, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())
	currentTime := time.Now()
	scheduleTime := currentTime.Add(-time.Hour)
	window, _ := models.NewWindow(2, "d", "1h", "24h")
	job := &scheduler.Job{
		Name:   "jobname",
		Tenant: tnnt,
		Task: &scheduler.Task{
			Name:   "pluginName",
			Config: nil,
		},
		Hooks:  nil,
		Window: window,
		Assets: nil,
	}
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

		t.Run("should give error if tenant service getDetails fails", func(t *testing.T) {
			pluginRepo := new(mockPluginRepo)
			pluginRepo.On("GetByName", "pluginName").Return(nil, fmt.Errorf("error in getting plugin by name"))
			defer pluginRepo.AssertExpectations(t)

			contextForTask := map[string]any{}

			//, systemEnvVars map[string]string, scheduledAt time.Time, contextForTask map[string]interface{}
			jobRunAssetsCompiler := service.NewJobAssetsCompiler(nil, pluginRepo)
			assets, err := jobRunAssetsCompiler.CompileJobRunAssets(ctx, job, systemEnvVars, scheduleTime, contextForTask)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "error in getting plugin by name")
			assert.Nil(t, assets)
		})

	})
}

type mockPluginRepo struct {
	mock.Mock
}

func (m *mockPluginRepo) GetByName(name string) (*models.Plugin, error) {
	args := m.Called(name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*models.Plugin), args.Error(1)
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
