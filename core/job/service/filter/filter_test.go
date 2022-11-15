package filter_test

import (
	"testing"

	"github.com/odpf/optimus/core/job/service/filter"
	"github.com/stretchr/testify/assert"
)

func TestFilter(t *testing.T) {
	t.Run("GetValue", func(t *testing.T) {
		t.Run("return project_name if operand project name exist", func(t *testing.T) {
			f := filter.NewFilter(filter.With(filter.ProjectName, "project-a"))
			actual := f.GetValue(filter.ProjectName)
			assert.Equal(t, "project-a", actual)
		})
		t.Run("return project_name if multi operand exist including project name", func(t *testing.T) {
			f := filter.NewFilter(
				filter.With(filter.JobName, "job-a"),
				filter.With(filter.ProjectName, "project-a"),
				filter.With(filter.ResourceDestination, "resource-destination"),
			)
			actual := f.GetValue(filter.ProjectName)
			assert.Equal(t, "project-a", actual)
		})
		t.Run("return empty string if operand project name empty", func(t *testing.T) {
			f := filter.NewFilter(filter.With(filter.ProjectName, ""))
			actual := f.GetValue(filter.ProjectName)
			assert.Empty(t, actual)
		})
		t.Run("return empty string if no operand exist", func(t *testing.T) {
			f := filter.NewFilter()
			actual := f.GetValue(filter.ProjectName)
			assert.Empty(t, actual)
		})
	})

	t.Run("Contains", func(t *testing.T) {
		t.Run("return true if non-empty operand is setted", func(t *testing.T) {
			f := filter.NewFilter(filter.With(filter.JobName, "job-a"))
			actual := f.Contains(filter.JobName)
			assert.True(t, actual)
		})
		t.Run("return true if multi non-empty operands are setted", func(t *testing.T) {
			f := filter.NewFilter(
				filter.With(filter.JobName, "job-a"),
				filter.With(filter.ProjectName, "project-a"),
				filter.With(filter.ResourceDestination, "resource-destination"),
			)
			actual := f.Contains(filter.JobName, filter.ProjectName)
			assert.True(t, actual)
		})
		t.Run("return false if one of multi operand not exist", func(t *testing.T) {
			f := filter.NewFilter(
				filter.With(filter.ProjectName, "project-a"),
				filter.With(filter.ResourceDestination, "resource-destination"),
			)
			actual := f.Contains(filter.JobName, filter.ProjectName)
			assert.False(t, actual)
		})
		t.Run("return false if empty operand is setted", func(t *testing.T) {
			f := filter.NewFilter(filter.With(filter.JobName, ""))
			actual := f.Contains(filter.JobName)
			assert.False(t, actual)
		})
		t.Run("return false if no operand is setted", func(t *testing.T) {
			f := filter.NewFilter()
			actual := f.Contains(filter.JobName)
			assert.False(t, actual)
		})
	})
}
