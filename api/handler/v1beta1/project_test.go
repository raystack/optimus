package v1beta1_test

import (
	"context"
	"errors"
	"testing"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"

	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
)

func TestProjectOnServer(t *testing.T) {
	log := log.NewNoop()
	ctx := context.Background()

	t.Run("RegisterProject", func(t *testing.T) {
		t.Run("should return error if saving to repository fails", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"BUCKET": "gs://some_folder",
				},
			}
			adapter := v1.NewAdapter(nil, nil)

			projectService := new(mock.ProjectService)
			projectService.On("Save", ctx, projectSpec).Return(errors.New("random error"))
			defer projectService.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"someVersion1.0",
				jobService, nil, nil,
				projectService,
				nil,
				nil,
				v1.NewAdapter(nil, nil),
				nil,
				nil,
				nil,
				nil,
			)

			projectRequest := pb.RegisterProjectRequest{Project: adapter.ToProjectProto(projectSpec)}
			resp, err := runtimeServiceServer.RegisterProject(context.Background(), &projectRequest)
			assert.Equal(t, "rpc error: code = Internal desc = random error: not able to register project a-data-project", err.Error())
			assert.Nil(t, resp)
		})
		t.Run("should register a project", func(t *testing.T) {
			projectName := "a-data-project"

			projectSpec := models.ProjectSpec{
				Name: projectName,
				Config: map[string]string{
					"BUCKET": "gs://some_folder",
				},
			}
			adapter := v1.NewAdapter(nil, nil)

			projectService := new(mock.ProjectService)
			projectService.On("Save", ctx, projectSpec).Return(nil)
			defer projectService.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				"someVersion1.0",
				jobService, nil, nil,
				projectService,
				nil,
				nil,
				v1.NewAdapter(nil, nil),
				nil,
				nil,
				nil,
				nil,
			)

			projectRequest := pb.RegisterProjectRequest{Project: adapter.ToProjectProto(projectSpec)}
			resp, err := runtimeServiceServer.RegisterProject(context.Background(), &projectRequest)
			assert.Nil(t, err)
			assert.Equal(t, &pb.RegisterProjectResponse{
				Success: true,
				Message: "project saved successfully.",
			}, resp)
		})
	})
}
