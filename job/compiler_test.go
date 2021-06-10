package job_test

import (
	"testing"
	"time"

	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

func TestCompiler(t *testing.T) {
	execUnit := new(mock.TaskPlugin)

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
		Dependencies: map[string]models.JobSpecDependency{},
	}

	t.Run("Compile", func(t *testing.T) {
		t.Run("should compile template without any error", func(t *testing.T) {
			com := job.NewCompiler(
				[]byte("content = {{.Job.Name}}"),
				"",
			)
			dag, err := com.Compile(namespaceSpec, spec)

			assert.Equal(t, dag.Contents, []byte("content = foo"))
			assert.Nil(t, err)
		})
		t.Run("should return error if failed to read template", func(t *testing.T) {
			com := job.NewCompiler(
				[]byte(""),
				"",
			)
			_, err := com.Compile(namespaceSpec, spec)
			assert.Equal(t, err, job.ErrEmptyTemplateFile)
		})
		t.Run("should return error if failed to parse template", func(t *testing.T) {
			com := job.NewCompiler(
				[]byte("content = {{.Tob.Name}}"),
				"",
			)
			_, err := com.Compile(namespaceSpec, spec)
			assert.Error(t, err)
		})
	})
}
