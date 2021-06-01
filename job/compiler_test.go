package job_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/odpf/optimus/core/fs"
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
		templatePath := "./template.py"

		t.Run("should compile template without any error", func(t *testing.T) {
			templateFile := new(mock.HTTPFile)
			templateFile.On("Read").Return(bytes.NewBufferString("sometemplate file"), nil)
			templateFile.On("Close").Return(nil)
			defer templateFile.AssertExpectations(t)

			fsm := new(mock.HTTPFileSystem)
			fsm.On("Open", templatePath).Return(templateFile, nil)
			defer fsm.AssertExpectations(t)

			com := job.NewCompiler(
				fsm,
				templatePath,
				"",
			)
			dag, err := com.Compile(namespaceSpec, spec)

			assert.Equal(t, dag.Contents, []byte("sometemplate file"))
			assert.Nil(t, err)
		})
		t.Run("should return error if failed to read file of a template at provided path", func(t *testing.T) {
			templateFile := new(mock.HTTPFile)
			templateFile.On("Read").Return(bytes.NewBufferString(""), fs.ErrNoSuchFile)
			templateFile.On("Close").Return(nil)
			defer templateFile.AssertExpectations(t)

			fsm := new(mock.HTTPFileSystem)
			fsm.On("Open", templatePath).Return(templateFile, nil)
			defer fsm.AssertExpectations(t)

			com := job.NewCompiler(
				fsm,
				templatePath,
				"",
			)
			_, err := com.Compile(namespaceSpec, spec)
			assert.Equal(t, err, job.ErrEmptyTemplateFile)
		})
	})
}
