package v1beta1_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	v1 "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestRuntimeServiceServer(t *testing.T) {
	log := log.NewNoop()
	ctx := context.Background()

	t.Run("Version", func(t *testing.T) {
		t.Run("should save specs and return with data", func(t *testing.T) {
			Version := "1.0.1"

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				nil, nil, nil,
			)
			versionRequest := pb.VersionRequest{Client: Version}
			resp, err := runtimeServiceServer.Version(context.Background(), &versionRequest)
			assert.Nil(t, err)
			assert.Equal(t, Version, resp.Server)
			assert.Equal(t, &pb.VersionResponse{Server: Version}, resp)
		})
	})

	t.Run("RegisterJobEvent", func(t *testing.T) {
		t.Run("should register the event if valid inputs", func(t *testing.T) {
			Version := "1.0.0"

			projectSpec := models.ProjectSpec{
				ID:   models.ProjectID(uuid.New()),
				Name: "a-data-project",
			}

			namespaceSpec := models.NamespaceSpec{
				ID:          uuid.New(),
				Name:        "game_jam",
				ProjectSpec: projectSpec,
			}

			jobSpecs := []models.JobSpec{
				{
					Name: "transform-tables",
				},
			}

			namespaceService := new(mock.NamespaceService)
			namespaceService.On("Get", ctx, projectSpec.Name, namespaceSpec.Name).Return(namespaceSpec, nil)
			defer namespaceService.AssertExpectations(t)

			jobService := new(mock.JobService)
			jobService.On("GetByName", ctx, jobSpecs[0].Name, namespaceSpec).Return(jobSpecs[0], nil)
			defer jobService.AssertExpectations(t)

			eventValues, _ := structpb.NewStruct(
				map[string]interface{}{
					"url": "https://example.io",
				},
			)
			eventSvc := new(mock.EventService)
			eventSvc.On("Register", ctx, namespaceSpec, jobSpecs[0], models.JobEvent{
				Type:  models.JobEventTypeFailure,
				Value: eventValues.GetFields(),
			}).Return(nil)
			defer eventSvc.AssertExpectations(t)

			runtimeServiceServer := v1.NewRuntimeServiceServer(
				log,
				Version,
				jobService, eventSvc, namespaceService,
			)
			req := &pb.RegisterJobEventRequest{
				ProjectName:   projectSpec.Name,
				JobName:       jobSpecs[0].Name,
				NamespaceName: namespaceSpec.Name,
				Event: &pb.JobEvent{
					Type:  pb.JobEvent_TYPE_FAILURE,
					Value: eventValues,
				},
			}
			_, err := runtimeServiceServer.RegisterJobEvent(ctx, req)
			assert.Nil(t, err)
		})
	})
}
