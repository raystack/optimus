package dag_test

import (
	_ "embed"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/job_run"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/scheduler/airflow/dag"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

//go:embed expected_dag.py
var compiledTemplate []byte

func TestDagCompiler(t *testing.T) {
	t.Run("Compile", func(t *testing.T) {
		repo := setupPluginRepo()
		tnnt, err := tenant.NewTenant("example-proj", "billing")
		assert.Nil(t, err)

		t.Run("should compile basic template without any error", func(t *testing.T) {
			com, err := dag.NewDagCompiler("http://optimus.example.com", repo)
			assert.Nil(t, err)

			job := setupJob(tnnt)
			compiledDag, err := com.Compile(job)
			assert.Nil(t, err)
			assert.Equal(t, string(compiledTemplate), string(compiledDag))
		})
	})
}

func setupJob(tnnt tenant.Tenant) *job_run.Job {
	window, err := models.NewWindow(1, "d", "0", "1h")
	if err != nil {
		panic(err)
	}
	schedule := &job_run.Schedule{
		StartDate:     time.Date(2022, 11, 10, 5, 2, 0, 0, time.UTC),
		EndDate:       nil,
		Interval:      "0 2 * * 0",
		DependsOnPast: false,
		CatchUp:       false,
		Retry: &job_run.Retry{
			Count:              2,
			Delay:              100,
			ExponentialBackoff: true,
		},
	}

	alert := &job_run.Alert{
		On:       job_run.SLAMissEvent,
		Channels: []string{"#alerts"},
		Config:   map[string]string{"duration": "2h"},
	}

	hooks := []*job_run.Hook{
		{Name: "transporter"},
		{Name: "predator"},
		{Name: "failureHook"},
	}

	//		Version:     1,
	//		Owner:       "infra-team@example.com",
	//		Description: "This job collects the billing information related to infrastructure",
	//		Labels:      map[string]string{"approved-by": "cto@example.com", "orchestrator": "optimus"},
	//		Schedule:    schedule,
	//		Alerts:      []*job_run.Alert{alert},
	//		Upstream:    nil,

	return &job_run.Job{
		Name:        "infra.billing.weekly-status-reports",
		Tenant:      tnnt,
		Destination: "bigquery://billing:reports.weekly-status",
		Task:        &job_run.Task{Name: "bq-bq"},
		Hooks:       hooks,
		Window:      window,
		Assets:      nil,
	}
}

type mockPluginRepo struct {
	plugins []*models.Plugin
}

func (m mockPluginRepo) GetByName(name string) (*models.Plugin, error) {
	for _, plugin := range m.plugins {
		if plugin.Info().Name == name {
			return plugin, nil
		}
	}
	return nil, fmt.Errorf("error finding %s", name)
}

func setupPluginRepo() mockPluginRepo {
	execUnit := new(mock.BasePlugin)
	execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:  "bq-bq",
		Image: "example.io/namespace/bq2bq-executor:latest",
	}, nil)

	transporterHook := "transporter"
	hookUnit := new(mock.BasePlugin)
	hookUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:      transporterHook,
		HookType:  models.HookTypePre,
		Image:     "example.io/namespace/transporter-executor:latest",
		DependsOn: []string{"predator"},
	}, nil)

	predatorHook := "predator"
	hookUnit2 := new(mock.BasePlugin)
	hookUnit2.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:     predatorHook,
		HookType: models.HookTypePost,
		Image:    "example.io/namespace/predator-image:latest",
	}, nil)

	hookUnit3 := new(mock.BasePlugin)
	hookUnit3.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:     "failureHook",
		HookType: models.HookTypeFail,
		Image:    "example.io/namespace/failure-hook-image:latest",
	}, nil)

	repo := mockPluginRepo{plugins: []*models.Plugin{
		{Base: execUnit}, {Base: hookUnit}, {Base: hookUnit2}, {Base: hookUnit3},
	}}
	return repo
}
