package v1beta1_test

import (
	"context"
	"errors"
	"testing"

	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"

	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
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

			projectService := new(mock.ProjectService)
			projectService.On("Save", ctx, projectSpec).Return(errors.New("random error"))
			defer projectService.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			projectServiceServer := v1.NewProjectServiceServer(
				log,
				projectService,
			)

			projectRequest := pb.RegisterProjectRequest{Project: v1.ToProjectProto(projectSpec)}
			resp, err := projectServiceServer.RegisterProject(ctx, &projectRequest)
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

			projectService := new(mock.ProjectService)
			projectService.On("Save", ctx, projectSpec).Return(nil)
			defer projectService.AssertExpectations(t)

			jobService := new(mock.JobService)
			defer jobService.AssertExpectations(t)

			projectServiceServer := v1.NewProjectServiceServer(
				log,
				projectService,
			)

			projectRequest := pb.RegisterProjectRequest{Project: v1.ToProjectProto(projectSpec)}
			resp, err := projectServiceServer.RegisterProject(ctx, &projectRequest)
			assert.Nil(t, err)
			assert.Equal(t, &pb.RegisterProjectResponse{
				Success: true,
				Message: "project saved successfully.",
			}, resp)
		})
	})

	t.Run("GetProject", func(t *testing.T) {
		projectName := "a-data-project"

		t.Run("should return nil and error if there's error when getting project", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(models.ProjectSpec{}, errors.New("random error"))
			projectServiceServer := v1.NewProjectServiceServer(
				log,
				projectService,
			)

			projectRequest := pb.GetProjectRequest{
				ProjectName: projectName,
			}
			actualResponse, actualErr := projectServiceServer.GetProject(ctx, &projectRequest)

			assert.Nil(t, actualResponse)
			assert.Error(t, actualErr)
		})

		t.Run("should return value and nil if no error is encountered", func(t *testing.T) {
			projectService := new(mock.ProjectService)
			projectService.On("Get", ctx, projectName).Return(models.ProjectSpec{}, nil)
			projectServiceServer := v1.NewProjectServiceServer(
				log,
				projectService,
			)

			projectRequest := pb.GetProjectRequest{
				ProjectName: projectName,
			}
			actualResponse, actualErr := projectServiceServer.GetProject(ctx, &projectRequest)

			assert.NotNil(t, actualResponse)
			assert.NoError(t, actualErr)
		})
	})
}
