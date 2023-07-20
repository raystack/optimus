package tenant_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/raystack/optimus/core/tenant"
)

func TestEntityProject(t *testing.T) {
	t.Run("ProjectName", func(t *testing.T) {
		t.Run("returns error in create if name is empty", func(t *testing.T) {
			_, err := tenant.ProjectNameFrom("")
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity project: project name is empty")
		})
		t.Run("return project name when valid", func(t *testing.T) {
			name, err := tenant.ProjectNameFrom("proj-optimus")
			assert.Nil(t, err)

			assert.Equal(t, "proj-optimus", name.String())
		})
	})

	t.Run("Project", func(t *testing.T) {
		t.Run("fails to create if name is empty", func(t *testing.T) {
			project, err := tenant.NewProject("", map[string]string{"a": "b"})

			assert.Nil(t, project)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity project: project name is empty")
		})
		t.Run("fails to create if config is empty", func(t *testing.T) {
			project, err := tenant.NewProject("name", map[string]string{})

			assert.Nil(t, project)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity project: missing mandatory configuration")
		})
		t.Run("creates a project model", func(t *testing.T) {
			project, err := tenant.NewProject("t-optimus", map[string]string{
				tenant.ProjectSchedulerHost:  "b",
				tenant.ProjectStoragePathKey: "d",
			})
			assert.Nil(t, err)

			assert.NotNil(t, project)
			assert.Equal(t, "t-optimus", project.Name().String())

			assert.NotNil(t, project.GetConfigs())

			val1, err := project.GetConfig(tenant.ProjectSchedulerHost)
			assert.Nil(t, err)
			assert.Equal(t, "b", val1)

			val2, err := project.GetConfig(tenant.ProjectStoragePathKey)
			assert.Nil(t, err)
			assert.Equal(t, "d", val2)
		})
	})
}
