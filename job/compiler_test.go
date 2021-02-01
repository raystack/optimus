package job_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/core/fs"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestCompiler(t *testing.T) {
	execUnit := new(mock.ExecutionUnit)
	execUnit.On("GetName").Return("bq")

	projSpec := models.ProjectSpec{
		Name: "foo-project",
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
			dag, err := com.Compile(spec, projSpec)

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
			_, err := com.Compile(spec, projSpec)
			assert.Equal(t, err, job.ErrEmptyTemplateFile)
		})
	})
}
