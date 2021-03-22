// +build !unit_test

package integration_tests

import (
	"bytes"
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/ext/scheduler/airflow"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/resources"
)

func TestCompiler(t *testing.T) {
	execUnit := new(mock.ExecutionUnit)
	execUnit.On("GetName").Return("bq")
	execUnit.On("GetImage").Return("example.io/namespace/image:latest")

	transporterHook := "transporter"
	hookUnit := new(mock.HookUnit)
	hookUnit.On("GetName").Return(transporterHook)
	hookUnit.On("GetImage").Return("example.io/namespace/hook-image:latest")
	hookUnit.On("GetType").Return(models.HookTypePre)

	predatorHook := "predator"
	hookUnit2 := new(mock.HookUnit)
	hookUnit2.On("GetName").Return(predatorHook)
	hookUnit2.On("GetImage").Return("example.io/namespace/predator-image:latest")
	hookUnit2.On("GetType").Return(models.HookTypePost)

	projSpec := models.ProjectSpec{
		Name: "foo-project",
	}

	externalProjSpec := models.ProjectSpec{
		Name: "foo-external-project",
	}

	depSpecIntra := models.JobSpec{
		Name:  "foo-intra-dep-job",
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
			Unit:     execUnit,
			Priority: 2000,
			Window: models.JobSpecTaskWindow{
				Size:       time.Hour,
				Offset:     0,
				TruncateTo: "d",
			},
		},
	}

	depSpecInter := models.JobSpec{
		Name:  "foo-inter-dep-job",
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
			Unit:     execUnit,
			Priority: 2000,
			Window: models.JobSpecTaskWindow{
				Size:       time.Hour,
				Offset:     0,
				TruncateTo: "d",
			},
		},
	}

	scheduleEndDate := time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC)
	hook1 := models.JobSpecHook{
		Config: []models.JobSpecConfigItem{
			{
				Name:  "FILTER_EXPRESSION",
				Value: "event_timestamp > 10000",
			},
		},
		Unit:      hookUnit,
		DependsOn: nil,
	}
	hook2 := models.JobSpecHook{
		Config: []models.JobSpecConfigItem{
			{
				Name:  "FILTER_EXPRESSION2",
				Value: "event_timestamp > 10000",
			},
		},
		Unit:      hookUnit2,
		DependsOn: []*models.JobSpecHook{&hook1},
	}
	spec := models.JobSpec{
		Name:  "foo",
		Owner: "mee@mee",
		Behavior: models.JobSpecBehavior{
			CatchUp:       true,
			DependsOnPast: false,
		},
		Schedule: models.JobSpecSchedule{
			StartDate: time.Date(2000, 11, 11, 0, 0, 0, 0, time.UTC),
			EndDate:   &scheduleEndDate,
			Interval:  "* * * * *",
		},
		Task: models.JobSpecTask{
			Unit:     execUnit,
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
		Hooks: []models.JobSpecHook{hook1, hook2},
		Labels: []models.JobSpecLabelItem{
			{
				Name:  "orchestrator",
				Value: "optimus",
			},
		},
	}

	t.Run("Compile", func(t *testing.T) {
		templatePath := "./resources/pack/templates/scheduler/airflow_1/base_dag.py"
		compiledTemplateOutput := "./expected_compiled_template.py"

		t.Run("should compile template without any error", func(t *testing.T) {
			// read scheduler template file
			scheduler := airflow.NewScheduler(resources.FileSystem, nil, nil)
			airflowTemplateFile, err := resources.FileSystem.Open(scheduler.GetTemplatePath())
			assert.Nil(t, err)
			defer airflowTemplateFile.Close()
			templateContent, err := ioutil.ReadAll(airflowTemplateFile)
			assert.Nil(t, err)

			templateFile := new(mock.HTTPFile)
			templateFile.On("Read").Return(bytes.NewBufferString(string(templateContent)), nil)
			templateFile.On("Close").Return(nil)
			defer templateFile.AssertExpectations(t)

			fsm := new(mock.HTTPFileSystem)
			fsm.On("Open", templatePath).Return(templateFile, nil)
			defer fsm.AssertExpectations(t)

			com := job.NewCompiler(
				fsm,
				templatePath,
				"http://airflow.io",
			)
			job, err := com.Compile(spec, projSpec)
			assert.Nil(t, err)
			expectedCompiledOutput, err := ioutil.ReadFile(compiledTemplateOutput)
			assert.Nil(t, err)
			assert.Equal(t, string(expectedCompiledOutput), string(job.Contents))

		})
	})
}
