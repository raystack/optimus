package task

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/odpf/optimus/core/logger"

	"github.com/golang/protobuf/ptypes"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/models"
)

// GRPCClient will be used by core to talk over grpc with plugins
type GRPCClient struct {
	client             pb.TaskPluginClient
	projectSpecAdapter ProjectSpecAdapter

	// Plugin name, used in filtering project secrets
	Name string
}

func (m *GRPCClient) GetTaskSchema(ctx context.Context, _ models.GetTaskSchemaRequest) (models.GetTaskSchemaResponse, error) {
	resp, err := m.client.GetTaskSchema(ctx, &pb.GetTaskSchema_Request{})
	if err != nil {
		ifFailToReachServerThenCrash(err)
		return models.GetTaskSchemaResponse{}, err
	}
	return models.GetTaskSchemaResponse{
		Name:        resp.Name,
		Description: resp.Description,
		Image:       resp.Image,
		SecretPath:  resp.SecretPath,
	}, nil
}

func (m *GRPCClient) GetTaskQuestions(ctx context.Context, request models.GetTaskQuestionsRequest) (models.GetTaskQuestionsResponse, error) {
	resp, err := m.client.GetTaskQuestions(ctx, &pb.GetTaskQuestions_Request{
		Options: &pb.PluginOptions{DryRun: request.DryRun},
	})
	if err != nil {
		return models.GetTaskQuestionsResponse{}, err
	}
	var questions []models.PluginQuestion
	for _, q := range resp.Questions {
		questions = append(questions, adaptQuestionFromProto(q))
	}
	return models.GetTaskQuestionsResponse{
		Questions: questions,
	}, nil
}

func (m *GRPCClient) ValidateTaskQuestion(ctx context.Context, request models.ValidateTaskQuestionRequest) (models.ValidateTaskQuestionResponse, error) {
	resp, err := m.client.ValidateTaskQuestion(ctx, &pb.ValidateTaskQuestion_Request{
		Options: &pb.PluginOptions{DryRun: request.DryRun},
		Answer: &pb.PluginAnswer{
			Question: adaptQuestionToProto(request.Answer.Question),
			Value:    request.Answer.Value,
		},
	})
	if err != nil {
		return models.ValidateTaskQuestionResponse{}, err
	}
	return models.ValidateTaskQuestionResponse{
		Success: resp.Success,
		Error:   resp.Error,
	}, nil
}

func (m *GRPCClient) DefaultTaskConfig(ctx context.Context, request models.DefaultTaskConfigRequest) (models.DefaultTaskConfigResponse, error) {
	answers := []*pb.PluginAnswer{}
	for _, a := range request.Answers {
		answers = append(answers, &pb.PluginAnswer{
			Question: adaptQuestionToProto(a.Question),
			Value:    a.Value,
		})
	}
	resp, err := m.client.DefaultTaskConfig(ctx, &pb.DefaultTaskConfig_Request{
		Options: &pb.PluginOptions{DryRun: request.DryRun},
		Answers: answers,
	})
	if err != nil {
		return models.DefaultTaskConfigResponse{}, err
	}
	return models.DefaultTaskConfigResponse{
		Config: AdaptConfigsFromProto(resp.Configs),
	}, nil
}

func (m *GRPCClient) DefaultTaskAssets(ctx context.Context, request models.DefaultTaskAssetsRequest) (models.DefaultTaskAssetsResponse, error) {
	answers := []*pb.PluginAnswer{}
	for _, a := range request.Answers {
		answers = append(answers, &pb.PluginAnswer{
			Question: adaptQuestionToProto(a.Question),
			Value:    a.Value,
		})
	}
	resp, err := m.client.DefaultTaskAssets(ctx, &pb.DefaultTaskAssets_Request{
		Options: &pb.PluginOptions{DryRun: request.DryRun},
		Answers: answers,
	})
	if err != nil {
		return models.DefaultTaskAssetsResponse{}, err
	}
	return models.DefaultTaskAssetsResponse{
		Assets: adaptAssetsFromProto(resp.Assets),
	}, nil
}

func (m *GRPCClient) CompileTaskAssets(ctx context.Context, request models.CompileTaskAssetsRequest) (models.CompileTaskAssetsResponse, error) {
	schdAt, err := ptypes.TimestampProto(request.InstanceSchedule)
	if err != nil {
		return models.CompileTaskAssetsResponse{}, err
	}
	instanceData := []*pb.InstanceSpecData{}
	for _, inst := range request.InstanceData {
		instanceData = append(instanceData, &pb.InstanceSpecData{
			Name:  inst.Name,
			Value: inst.Value,
			Type:  pb.InstanceSpecData_Type(pb.InstanceSpecData_Type_value[strings.ToUpper(inst.Type)]),
		})
	}

	resp, err := m.client.CompileTaskAssets(ctx, &pb.CompileTaskAssets_Request{
		JobConfigs: AdaptConfigsToProto(request.Config),
		JobAssets:  adaptAssetsToProto(request.Assets),
		TaskWindow: &pb.TaskWindow{
			Size:       ptypes.DurationProto(request.TaskWindow.Size),
			Offset:     ptypes.DurationProto(request.TaskWindow.Offset),
			TruncateTo: request.TaskWindow.TruncateTo,
		},
		InstanceSchedule: schdAt,
		InstanceData:     instanceData,
		Options:          &pb.PluginOptions{DryRun: request.DryRun},
	})
	if err != nil {
		return models.CompileTaskAssetsResponse{}, err
	}
	return models.CompileTaskAssetsResponse{
		Assets: adaptAssetsFromProto(resp.Assets),
	}, nil
}

func (m *GRPCClient) GenerateTaskDestination(ctx context.Context, request models.GenerateTaskDestinationRequest) (models.GenerateTaskDestinationResponse, error) {
	resp, err := m.client.GenerateTaskDestination(ctx, &pb.GenerateTaskDestination_Request{
		JobConfig: AdaptConfigsToProto(request.Config),
		JobAssets: adaptAssetsToProto(request.Assets),
		Project:   m.projectSpecAdapter.ToProjectProtoWithSecret(request.Project, models.InstanceTypeTask, m.Name),
		Options:   &pb.PluginOptions{DryRun: request.DryRun},
	})
	if err != nil {
		return models.GenerateTaskDestinationResponse{}, err
	}
	return models.GenerateTaskDestinationResponse{
		Destination: resp.Destination,
	}, nil
}

func (m *GRPCClient) GenerateTaskDependencies(ctx context.Context, request models.GenerateTaskDependenciesRequest) (models.GenerateTaskDependenciesResponse, error) {
	resp, err := m.client.GenerateTaskDependencies(ctx, &pb.GenerateTaskDependencies_Request{
		JobConfig: AdaptConfigsToProto(request.Config),
		JobAssets: adaptAssetsToProto(request.Assets),
		Project:   m.projectSpecAdapter.ToProjectProtoWithSecret(request.Project, models.InstanceTypeTask, m.Name),
		Options:   &pb.PluginOptions{DryRun: request.DryRun},
	})
	if err != nil {
		return models.GenerateTaskDependenciesResponse{}, err
	}
	return models.GenerateTaskDependenciesResponse{
		Dependencies: resp.Dependencies,
	}, nil
}

func ifFailToReachServerThenCrash(err error) {
	if strings.Contains(err.Error(), "connection refused") && strings.Contains(err.Error(), "dial unix") {
		logger.E(fmt.Sprintf("plugin communication failed with: %s", err.Error()))
		// TODO(kush.sharma): once plugins are more stable, remove this fail
		os.Exit(1)
	}
}
