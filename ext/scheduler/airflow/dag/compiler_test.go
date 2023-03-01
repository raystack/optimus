package dag_test

import (
	_ "embed"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/scheduler"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/scheduler/airflow/dag"
	"github.com/odpf/optimus/internal/models"
	"github.com/odpf/optimus/sdk/plugin"
	"github.com/odpf/optimus/sdk/plugin/mock"
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
			compiledDag, err := com.Compile(job)
			assert.Nil(t, err)
			assert.Equal(t, string(compiledTemplate), string(compiledDag))
		})
	})
}

func setupJobDetails(tnnt tenant.Tenant) *scheduler.JobWithDetails {
	window, err := models.NewWindow(1, "d", "0", "1h")
	if err != nil {
		panic(err)
	}
	end := time.Date(2022, 11, 10, 10, 2, 0, 0, time.UTC)
	schedule := &scheduler.Schedule{
		StartDate:     time.Date(2022, 11, 10, 5, 2, 0, 0, time.UTC),
		EndDate:       &end,
		Interval:      "0 2 * * 0",
		DependsOnPast: false,
		CatchUp:       true,
	}

	retry := scheduler.Retry{
		Count:              2,
		Delay:              100,
		ExponentialBackoff: true,
	}

	alert := scheduler.Alert{
		On:       scheduler.EventCategorySLAMiss,
		Channels: []string{"#alerts"},
		Config:   map[string]string{"duration": "2h"},
	}

	hooks := []*scheduler.Hook{
		{Name: "transporter"},
		{Name: "predator"},
		{Name: "failureHook"},
	}

	jobMeta := &scheduler.JobMetadata{
		Version:     1,
		Owner:       "infra-team@example.com",
		Description: "This job collects the billing information related to infrastructure",
		Labels:      map[string]string{"orchestrator": "optimus"},
	}

	jobName := scheduler.JobName("infra.billing.weekly-status-reports")
	job := &scheduler.Job{
		Name:        jobName,
		Tenant:      tnnt,
		Destination: "bigquery://billing:reports.weekly-status",
		Task:        &scheduler.Task{Name: "bq-bq"},
		Hooks:       hooks,
		Window:      window,
		Assets:      nil,
	}

	runtimeConfig := scheduler.RuntimeConfig{
		Resource: &scheduler.Resource{
			Limit: &scheduler.ResourceConfig{
				CPU:    "200m",
				Memory: "2G",
			},
		},
		Scheduler: map[string]string{"pool": "billing"},
	}

	tnnt1, _ := tenant.NewTenant("project", "namespace")
	tnnt2, _ := tenant.NewTenant("external-project", "external-namespace")
	upstreams := scheduler.Upstreams{
		HTTP: nil,
		UpstreamJobs: []*scheduler.JobUpstream{
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
				JobName:  "foo-external-optimus-dep-job",
				Host:     "http://optimus.external.io",
				TaskName: "bq-bq",
				Tenant:   tnnt2,
				External: true,
				State:    "resolved",
			},
		},
	}

	return &scheduler.JobWithDetails{
		Name:        jobName,
		Job:         job,
		JobMetadata: jobMeta,
		Schedule:    schedule,
		Retry:       retry,
		Alerts:      []scheduler.Alert{alert},

		RuntimeConfig: runtimeConfig,
		Upstreams:     upstreams,
		Priority:      2000,
	}
}

type mockPluginRepo struct {
	plugins []*plugin.Plugin
}

func (m mockPluginRepo) GetByName(name string) (*plugin.Plugin, error) {
	for _, plugin := range m.plugins {
		if plugin.Info().Name == name {
			return plugin, nil
		}
	}
	return nil, fmt.Errorf("error finding %s", name)
}

func setupPluginRepo() mockPluginRepo {
	execUnit := new(mock.YamlMod)
	execUnit.On("PluginInfo").Return(&plugin.Info{
		Name:  "bq-bq",
		Image: "example.io/namespace/bq2bq-executor:latest",
		Entrypoint: plugin.Entrypoint{
			Cmds: []string{"/bin/bash", "-c"},
			Args: []string{"python3 /opt/bumblebee/main.py"},
		},
	}, nil)

	transporterHook := "transporter"
	hookUnit := new(mock.YamlMod)
	hookUnit.On("PluginInfo").Return(&plugin.Info{
		Name:     transporterHook,
		HookType: plugin.HookTypePre,
		Image:    "example.io/namespace/transporter-executor:latest",
		Entrypoint: plugin.Entrypoint{
			Cmds: []string{"/bin/sh", "-c"},
			Args: []string{"java -cp /opt/transporter/transporter.jar:/opt/transporter/jolokia-jvm-agent.jar -javaagent:jolokia-jvm-agent.jar=port=7777,host=0.0.0.0 com.gojek.transporter.Main"},
		},
		DependsOn: []string{"predator"},
	}, nil)

	predatorHook := "predator"
	hookUnit2 := new(mock.YamlMod)
	hookUnit2.On("PluginInfo").Return(&plugin.Info{
		Name:     predatorHook,
		HookType: plugin.HookTypePost,
		Image:    "example.io/namespace/predator-image:latest",
		Entrypoint: plugin.Entrypoint{
			Cmds: []string{"/bin/sh", "-c"},
			Args: []string{"predator ${SUB_COMMAND} -s ${PREDATOR_URL} -u \"${BQ_PROJECT}.${BQ_DATASET}.${BQ_TABLE}\""},
		},
	}, nil)

	hookUnit3 := new(mock.YamlMod)
	hookUnit3.On("PluginInfo").Return(&plugin.Info{
		Name:     "failureHook",
		HookType: plugin.HookTypeFail,
		Image:    "example.io/namespace/failure-hook-image:latest",
		Entrypoint: plugin.Entrypoint{
			Cmds: []string{"/bin/sh", "-c"},
			Args: []string{"sleep 5"},
		},
	}, nil)

	repo := mockPluginRepo{plugins: []*plugin.Plugin{
		{YamlMod: execUnit}, {YamlMod: hookUnit}, {YamlMod: hookUnit2}, {YamlMod: hookUnit3},
	}}
	return repo
}
