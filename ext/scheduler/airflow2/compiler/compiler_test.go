package compiler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/ext/scheduler/airflow2/compiler"
	"github.com/odpf/optimus/models"
)

func TestCompiler(t *testing.T) {
	projSpec := models.ProjectSpec{
		Name: "foo-project",
	}
	namespaceSpec := models.NamespaceSpec{
		Name:        "foo-namespace",
		ProjectSpec: projSpec,
	}

	spec := models.JobSpec{
		Name:  "foo",
		Owner: "mee@mee",
		Behavior: models.JobSpecBehavior{
			CatchUp:       true,
			DependsOnPast: true,
			Retry: models.JobSpecBehaviorRetry{
				Count:              2,
				Delay:              time.Second * 2,
				ExponentialBackoff: false,
			},
			Notify: []models.JobSpecNotifier{
				{
					On: models.SLAMissEvent,
					Config: map[string]string{
						"duration": "2s",
					},
					Channels: []string{"scheme://route"},
				},
			},
		},
		Schedule: models.JobSpecSchedule{
			StartDate: time.Date(2000, 11, 11, 0, 0, 0, 0, time.UTC),
			Interval:  "* * * * *",
		},
		Task: models.JobSpecTask{
			Unit:     &models.Plugin{},
			Priority: 2000,
			Window: models.JobSpecTaskWindow{
				Size:       time.Hour,
				Offset:     0,
				TruncateTo: "d",
			},
		},
		Dependencies: map[string]models.JobSpecDependency{},
	}

	t.Run("Compile", func(t *testing.T) {
		t.Run("should compile template without any error", func(t *testing.T) {
			com := compiler.NewCompiler(
				"",
			)
			dag, err := com.Compile([]byte("content = {{.Job.Name}}"), namespaceSpec, spec)

			assert.Equal(t, dag.Contents, []byte("content = foo"))
			assert.Nil(t, err)
		})
		t.Run("should compile template without any error without notify channels", func(t *testing.T) {
			tempSpec := spec
			tempSpec.Behavior.Notify = []models.JobSpecNotifier{}
			com := compiler.NewCompiler(
				"",
			)
			dag, err := com.Compile([]byte("content = {{.Job.Name}}"), namespaceSpec, tempSpec)

			assert.Equal(t, dag.Contents, []byte("content = foo"))
			assert.Nil(t, err)
		})
		t.Run("should return error if failed to read template", func(t *testing.T) {
			com := compiler.NewCompiler(
				"",
			)
			_, err := com.Compile([]byte(""), namespaceSpec, spec)
			assert.Equal(t, err, compiler.ErrEmptyTemplateFile)
		})
		t.Run("should return error if failed to parse template", func(t *testing.T) {
			com := compiler.NewCompiler(
				"",
			)
			_, err := com.Compile([]byte("content = {{.Tob.Name}}"), namespaceSpec, spec)
			assert.Error(t, err)
		})
	})
}
