package filter_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/job/service/filter"
)

func TestFilter(t *testing.T) {
	t.Run("GetStringValue", func(t *testing.T) {
		t.Run("return project_name if operand project name exist", func(t *testing.T) {
			f := filter.NewFilter(filter.WithString(filter.ProjectName, "project-a"))
			actual := f.GetStringValue(filter.ProjectName)
			assert.Equal(t, "project-a", actual)
		})
		t.Run("return project_name if multi operand exist including project name", func(t *testing.T) {
			f := filter.NewFilter(
				filter.WithString(filter.JobName, "job-a"),
				filter.WithString(filter.ProjectName, "project-a"),
				filter.WithString(filter.ResourceDestination, "resource-destination"),
			)
			actual := f.GetStringValue(filter.ProjectName)
			assert.Equal(t, "project-a", actual)
		})
		t.Run("return empty string if operand project name empty", func(t *testing.T) {
			f := filter.NewFilter(filter.WithString(filter.ProjectName, ""))
			actual := f.GetStringValue(filter.ProjectName)
			assert.Empty(t, actual)
		})
		t.Run("return empty string if no operand exist", func(t *testing.T) {
			f := filter.NewFilter()
			actual := f.GetStringValue(filter.ProjectName)
			assert.Empty(t, actual)
		})
		t.Run("return empty string if only operand array string type exist", func(t *testing.T) {
			f := filter.NewFilter(
				filter.WithStringArray(filter.ProjectName, []string{"example"}),
			)
			actual := f.GetStringValue(filter.ProjectName)
			assert.Empty(t, actual)
		})
	})

	t.Run("GetStringArrayValue", func(t *testing.T) {
		t.Run("return namespace names if operand namespace names exist", func(t *testing.T) {
			f := filter.NewFilter(filter.WithStringArray(filter.NamespaceNames, []string{"example"}))
			actual := f.GetStringArrayValue(filter.NamespaceNames)
			assert.Equal(t, []string{"example"}, actual)
		})
		t.Run("return namespace names if multi operand exist including namespace names", func(t *testing.T) {
			f := filter.NewFilter(
				filter.WithStringArray(filter.NamespaceNames, []string{"example"}),
				filter.WithString(filter.JobName, "job-a"),
				filter.WithString(filter.ProjectName, "project-a"),
				filter.WithString(filter.ResourceDestination, "resource-destination"),
			)
			actual := f.GetStringArrayValue(filter.NamespaceNames)
			assert.Equal(t, []string{"example"}, actual)
		})
		t.Run("return nil if namespace names not exist", func(t *testing.T) {
			f := filter.NewFilter(
				filter.WithString(filter.JobName, "job-a"),
				filter.WithString(filter.ProjectName, "project-a"),
				filter.WithString(filter.ResourceDestination, "resource-destination"),
			)
			actual := f.GetStringArrayValue(filter.NamespaceNames)
			assert.Nil(t, actual)
		})
		t.Run("return nil if no operand exist", func(t *testing.T) {
			f := filter.NewFilter()
			actual := f.GetStringArrayValue(filter.NamespaceNames)
			assert.Nil(t, actual)
		})
		t.Run("return nil if only operand string type exist", func(t *testing.T) {
			f := filter.NewFilter(
				filter.WithString(filter.NamespaceNames, "example"),
			)
			actual := f.GetStringArrayValue(filter.NamespaceNames)
			assert.Nil(t, actual)
		})
	})

	t.Run("Contains", func(t *testing.T) {
		t.Run("return true if non-empty operand is setted", func(t *testing.T) {
			f := filter.NewFilter(filter.WithString(filter.JobName, "job-a"))
			actual := f.Contains(filter.JobName)
			assert.True(t, actual)
		})
		t.Run("return true if multi non-empty operands are setted", func(t *testing.T) {
			f := filter.NewFilter(
				filter.WithString(filter.JobName, "job-a"),
				filter.WithString(filter.ProjectName, "project-a"),
				filter.WithString(filter.ResourceDestination, "resource-destination"),
			)
			actual := f.Contains(filter.JobName, filter.ProjectName)
			assert.True(t, actual)
		})
		t.Run("return false if one of multi operand not exist", func(t *testing.T) {
			f := filter.NewFilter(
				filter.WithString(filter.ProjectName, "project-a"),
				filter.WithString(filter.ResourceDestination, "resource-destination"),
			)
			actual := f.Contains(filter.JobName, filter.ProjectName)
			assert.False(t, actual)
		})
		t.Run("return false if empty operand is setted", func(t *testing.T) {
			f := filter.NewFilter(filter.WithString(filter.JobName, ""))
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
