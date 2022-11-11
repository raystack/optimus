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

			job := setupJobDetails(tnnt)
			compiledDag, err := com.Compile(job, 2000)
			assert.Nil(t, err)
			assert.Equal(t, string(compiledTemplate), string(compiledDag))
		})
	})
}

func setupJobDetails(tnnt tenant.Tenant) *job_run.JobWithDetails {
	window, err := models.NewWindow(1, "d", "0", "1h")
	if err != nil {
		panic(err)
	}
	end := time.Date(2022, 11, 10, 10, 2, 0, 0, time.UTC)
	schedule := &job_run.Schedule{
		StartDate:     time.Date(2022, 11, 10, 5, 2, 0, 0, time.UTC),
		EndDate:       &end,
		Interval:      "0 2 * * 0",
		DependsOnPast: false,
		CatchUp:       true,
	}

	retry := &job_run.Retry{
		Count:              2,
		Delay:              100,
		ExponentialBackoff: true,
	}

	alert := job_run.Alert{
		On:       job_run.SLAMissEvent,
		Channels: []string{"#alerts"},
		Config:   map[string]string{"duration": "2h"},
	}

	hooks := []*job_run.Hook{
		{Name: "transporter"},
		{Name: "predator"},
		{Name: "failureHook"},
	}

	jobMeta := &job_run.JobMetadata{
		Version:     1,
		Owner:       "infra-team@example.com",
		Description: "This job collects the billing information related to infrastructure",
		Labels:      map[string]string{"orchestrator": "optimus"},
	}

	jobName := job_run.JobName("infra.billing.weekly-status-reports")
	job := &job_run.Job{
		Name:        jobName,
		Tenant:      tnnt,
		Destination: "bigquery://billing:reports.weekly-status",
		Task:        &job_run.Task{Name: "bq-bq"},
		Hooks:       hooks,
		Window:      window,
		Assets:      nil,
	}

	runtimeConfig := job_run.RuntimeConfig{
		Resource: &job_run.Resource{
			Limit: &job_run.ResourceConfig{
				CPU:    "200m",
				Memory: "2G",
			},
		},
		Scheduler: map[string]string{"pool": "billing"},
	}

	tnnt1, _ := tenant.NewTenant("project", "namespace")
	tnnt2, _ := tenant.NewTenant("external-project", "external-namespace")
	upstreams := job_run.Upstreams{
		HTTP: nil,
		Upstreams: []*job_run.Upstream{
			{
				Host:     "http://optimus.example.com",
				Tenant:   tnnt,
				JobName:  "foo-intra-dep-job",
				TaskName: "bq",
				State:    "resolved",
			},
			{
				Host:     "http://optimus.example.com",
				Tenant:   tnnt1,
				JobName:  "foo-inter-dep-job",
				TaskName: "bq-bq",
				State:    "resolved",
			},
			{
				Host:     "http://optimus.example.com",
				Tenant:   tnnt1,
				JobName:  "foo-inter-dep-job-unresolved",
				TaskName: "bq-bq",
				State:    "unresolved",
			},
			{
				Host:     "http://optimus.external.io",
				Tenant:   tnnt2,
				JobName:  "foo-external-optimus-dep-job",
				TaskName: "bq-bq",
				State:    "resolved",
			},
		},
	}

	return &job_run.JobWithDetails{
		Name:        jobName,
		Job:         job,
		JobMetadata: jobMeta,
		Schedule:    schedule,
		Retry:       retry,
		Alerts:      []job_run.Alert{alert},

		RuntimeConfig: runtimeConfig,
		Upstreams:     upstreams,
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
