package airflow2_test

import (
	_ "embed"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/ext/scheduler/airflow2"
	"github.com/odpf/optimus/ext/scheduler/airflow2/compiler"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

//go:embed resources/expected_compiled_template.py
var CompiledTemplate []byte

func TestCompilerIntegration(t *testing.T) {
	execUnit := new(mock.BasePlugin)
	execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:       "bq",
		Image:      "example.io/namespace/image:latest",
		SecretPath: "/opt/optimus/secrets/auth.json",
	}, nil)

	transporterHook := "transporter"
	hookUnit := new(mock.BasePlugin)
	hookUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:       transporterHook,
		HookType:   models.HookTypePre,
		Image:      "example.io/namespace/hook-image:latest",
		SecretPath: "/opt/optimus/secrets/auth.json",
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
		Name:     "hook-for-fail",
		HookType: models.HookTypeFail,
		Image:    "example.io/namespace/fail-image:latest",
	}, nil)

	projSpec := models.ProjectSpec{
		Name: "foo-project",
	}

	namespaceSpec := models.NamespaceSpec{
		Name:        "bar-namespace",
		ProjectSpec: projSpec,
	}

	externalProjSpec := models.ProjectSpec{
		Name: "foo-external-project",
	}

	externalProjNamespaceSpec := models.NamespaceSpec{
		Name:        "bar-namespace",
		ProjectSpec: externalProjSpec,
	}

	depSpecIntra := models.JobSpec{
		Name:  "foo-intra-dep-job",
		Owner: "mee@mee",
		Behavior: models.JobSpecBehavior{
			CatchUp:       true,
			DependsOnPast: true,
		},
		Schedule: models.JobSpecSchedule{
			StartDate: time.Date(2000, 11, 11, 0, 0, 0, 0, time.UTC),
			Interval:  "* * * * *",
		},
		Task: models.JobSpecTask{
			Unit:     &models.Plugin{Base: execUnit},
			Priority: 2000,
			Window: models.JobSpecTaskWindow{
				Size:       time.Hour,
				Offset:     0,
				TruncateTo: "d",
			},
		},
		NamespaceSpec: namespaceSpec,
	}

	depSpecInter := models.JobSpec{
		Name:  "foo-inter-dep-job",
		Owner: "mee@mee",
		Behavior: models.JobSpecBehavior{
			CatchUp:       true,
			DependsOnPast: true,
		},
		Schedule: models.JobSpecSchedule{
			StartDate: time.Date(2000, 11, 11, 0, 0, 0, 0, time.UTC),
			Interval:  "* * * * *",
		},
		Task: models.JobSpecTask{
			Unit:     &models.Plugin{Base: execUnit},
			Priority: 2000,
			Window: models.JobSpecTaskWindow{
				Size:       time.Hour,
				Offset:     0,
				TruncateTo: "d",
			},
		},
		NamespaceSpec: externalProjNamespaceSpec,
	}

	scheduleEndDate := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
	hook1 := models.JobSpecHook{
		Config: []models.JobSpecConfigItem{
			{
				Name:  "FILTER_EXPRESSION",
				Value: "event_timestamp > 10000",
			},
		},
		Unit:      &models.Plugin{Base: hookUnit},
		DependsOn: nil,
	}
	hook2 := models.JobSpecHook{
		Config: []models.JobSpecConfigItem{
			{
				Name:  "FILTER_EXPRESSION2",
				Value: "event_timestamp > 10000",
			},
		},
		Unit:      &models.Plugin{Base: hookUnit2},
		DependsOn: []*models.JobSpecHook{&hook1},
	}
	hook3 := models.JobSpecHook{
		Config: []models.JobSpecConfigItem{},
		Unit:   &models.Plugin{Base: hookUnit3},
	}
	spec := models.JobSpec{
		Name:  "foo",
		Owner: "mee@mee",
		Behavior: models.JobSpecBehavior{
			CatchUp:       true,
			DependsOnPast: true,
			Retry: models.JobSpecBehaviorRetry{
				Count:              4,
				Delay:              0,
				ExponentialBackoff: true,
			},
			Notify: []models.JobSpecNotifier{
				{
					On: models.SLAMissEvent, Config: map[string]string{
						"duration": "2h",
					},
				},
			},
		},
		Schedule: models.JobSpecSchedule{
			StartDate: time.Date(2000, 11, 11, 0, 0, 0, 0, time.UTC),
			EndDate:   &scheduleEndDate,
			Interval:  "* * * * *",
		},
		Task: models.JobSpecTask{
			Unit:     &models.Plugin{Base: execUnit},
			Priority: 2000,
			Window: models.JobSpecTaskWindow{
				Size:       time.Hour,
				Offset:     0,
				TruncateTo: "d",
			},
		},
		Dependencies: map[string]models.JobSpecDependency{
			// we'll add resolved dependencies
			"destination1": {Job: &depSpecIntra, Project: &projSpec, Type: models.JobSpecDependencyTypeIntra},
			"destination2": {Job: &depSpecInter, Project: &externalProjSpec, Type: models.JobSpecDependencyTypeInter},
		},
		Assets: *models.JobAssets{}.New(
			[]models.JobSpecAsset{
				{
					Name:  "query.sql",
					Value: "select * from 1",
				},
			},
		),
		Hooks: []models.JobSpecHook{hook1, hook2, hook3},
		Labels: map[string]string{
			"orchestrator": "optimus",
		},
	}

	t.Run("Compile", func(t *testing.T) {
		t.Run("should compile basic template without any error", func(t *testing.T) {
			scheduler := airflow2.NewScheduler(nil, nil, nil)
			com := compiler.NewCompiler(
				"http://airflow.example.io",
			)
			job, err := com.Compile(scheduler.GetTemplate(), namespaceSpec, spec)
			assert.Nil(t, err)
			assert.Equal(t, string(CompiledTemplate), string(job.Contents))
		})
	})
}
