package hook

import (
	"context"

	"github.com/odpf/optimus/plugin/task"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/models"
)

// GRPCClient will be used by core to talk over grpc with plugins
type GRPCClient struct {
	client             pb.HookPluginClient
	projectSpecAdapter ProjectSpecAdapter
}

func (m *GRPCClient) GetHookSchema(ctx context.Context, _ models.GetHookSchemaRequest) (models.GetHookSchemaResponse, error) {
	resp, err := m.client.GetHookSchema(ctx, &pb.GetHookSchema_Request{})
	if err != nil {
		return models.GetHookSchemaResponse{}, err
	}
	return models.GetHookSchemaResponse{
		Name:        resp.Name,
		Description: resp.Description,
		Image:       resp.Image,
		DependsOn:   resp.DependsOn,
		Type:        models.HookType(resp.Type),
		SecretPath:  resp.SecretPath,
	}, nil
}

func (m *GRPCClient) GetHookQuestions(ctx context.Context, request models.GetHookQuestionsRequest) (models.GetHookQuestionsResponse, error) {
	resp, err := m.client.GetHookQuestions(ctx, &pb.GetHookQuestions_Request{
		JobName: request.JobName,
		Options: &pb.PluginOptions{DryRun: request.DryRun},
	})
	if err != nil {
		return models.GetHookQuestionsResponse{}, err
	}
	var questions []models.PluginQuestion
	for _, q := range resp.Questions {
		questions = append(questions, adaptQuestionFromProto(q))
	}
	return models.GetHookQuestionsResponse{
		Questions: questions,
	}, nil
}

func (m *GRPCClient) ValidateHookQuestion(ctx context.Context, request models.ValidateHookQuestionRequest) (models.ValidateHookQuestionResponse, error) {
	resp, err := m.client.ValidateHookQuestion(ctx, &pb.ValidateHookQuestion_Request{
		Options: &pb.PluginOptions{DryRun: request.DryRun},
		Answer: &pb.PluginAnswer{
			Question: adaptQuestionToProto(request.Answer.Question),
			Value:    request.Answer.Value,
		},
	})
	if err != nil {
		return models.ValidateHookQuestionResponse{}, err
	}
	return models.ValidateHookQuestionResponse{
		Success: resp.Success,
		Error:   resp.Error,
	}, nil
}

func (m *GRPCClient) DefaultHookConfig(ctx context.Context, request models.DefaultHookConfigRequest) (models.DefaultHookConfigResponse, error) {
	answers := []*pb.PluginAnswer{}
	for _, a := range request.Answers {
		answers = append(answers, &pb.PluginAnswer{
			Question: adaptQuestionToProto(a.Question),
			Value:    a.Value,
		})
	}
	resp, err := m.client.DefaultHookConfig(ctx, &pb.DefaultHookConfig_Request{
		Options:     &pb.PluginOptions{DryRun: request.DryRun},
		Answers:     answers,
		TaskConfigs: task.AdaptConfigsToProto(request.TaskConfig),
	})
	if err != nil {
		return models.DefaultHookConfigResponse{}, err
	}
	return models.DefaultHookConfigResponse{
		Config: adaptConfigFromProto(resp.Config),
	}, nil
}

func (m *GRPCClient) DefaultHookAssets(ctx context.Context, request models.DefaultHookAssetsRequest) (models.DefaultHookAssetsResponse, error) {
	answers := []*pb.PluginAnswer{}
	for _, a := range request.Answers {
		answers = append(answers, &pb.PluginAnswer{
			Question: adaptQuestionToProto(a.Question),
			Value:    a.Value,
		})
	}
	resp, err := m.client.DefaultHookAssets(ctx, &pb.DefaultHookAssets_Request{
		Options:     &pb.PluginOptions{DryRun: request.DryRun},
		Answers:     answers,
		TaskConfigs: task.AdaptConfigsToProto(request.TaskConfig),
	})
	if err != nil {
		return models.DefaultHookAssetsResponse{}, err
	}
	return models.DefaultHookAssetsResponse{
		Assets: adaptAssetsFromProto(resp.Assets),
	}, nil
}
