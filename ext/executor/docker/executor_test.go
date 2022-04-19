//go:build !unit_test
// +build !unit_test

package docker_test

import (
	"context"
	"testing"

	"github.com/odpf/optimus/mock"

	"github.com/odpf/optimus/ext/executor/docker"
	"github.com/odpf/optimus/models"
	"github.com/stretchr/testify/assert"
)

// these will go as integration tests
func TestExecutor(t *testing.T) {
	ctx := context.Background()

	execUnit1 := new(mock.BasePlugin)
	execUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name:  "neo",
		Image: "ghcr.io/kushsharma/optimus-task-neo-executor:0.0.5",
	}, nil)

	t.Run("Start", func(t *testing.T) {
		t.Run("should create and start image container", func(t *testing.T) {
			ex := docker.NewExecutor(docker.EnvClientFactory{}, "localhost")
			resp, err := ex.Start(ctx, &models.ExecutorStartRequest{
				InstanceID: "test-id-1",
				JobName:    "job-1",
				JobLabels:  map[string]string{},
				Namespace: models.NamespaceSpec{
					Name: "ns-1",
					ProjectSpec: models.ProjectSpec{
						Name: "proj-1",
					},
				},
				Unit: &models.Plugin{Base: execUnit1},
				Config: models.JobSpecConfigs{
					{
						Name:  "foo",
						Value: "bar",
					},
				},
				Assets: models.JobAssets{},
				Type:   models.InstanceTypeTask,
			})
			assert.Nil(t, err)
			assert.NotNil(t, resp)
		})
	})
	t.Run("Stats", func(t *testing.T) {
		t.Run("should fetch stats of image container", func(t *testing.T) {
			ex := docker.NewExecutor(docker.EnvClientFactory{}, "localhost")
			resp, err := ex.Stats(ctx, "test-id-1")
			assert.Nil(t, err)
			assert.Equal(t, 0, resp.ExitCode)
		})
	})
}
