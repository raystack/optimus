package cli

import (
	"context"
	"time"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/plugin/v1beta1/base"
	pbp "github.com/odpf/optimus/protos/odpf/optimus/plugins/v1beta1"
)

const (
	PluginGRPCMaxRetry = 3
	BackoffDuration    = 200 * time.Millisecond
)

// GRPCClient will be used by core to talk over grpc with plugins
type GRPCClient struct {
	client pbp.CLIModServiceClient

	baseClient *base.GRPCClient
}

func (m *GRPCClient) PluginInfo() (*models.PluginInfoResponse, error) {
	return m.baseClient.PluginInfo()
}

func (m *GRPCClient) GetQuestions(ctx context.Context, request models.GetQuestionsRequest) (*models.GetQuestionsResponse, error) {
	resp, err := m.client.GetQuestions(ctx, &pbp.GetQuestionsRequest{
		JobName: request.JobName,
		Options: &pbp.PluginOptions{DryRun: request.DryRun},
	})
	if err != nil {
		return nil, err
	}
	var questions []models.PluginQuestion
	for _, q := range resp.Questions {
		questions = append(questions, AdaptQuestionFromProto(q))
	}
	return &models.GetQuestionsResponse{
		Questions: questions,
	}, nil
}

func (m *GRPCClient) ValidateQuestion(ctx context.Context, request models.ValidateQuestionRequest) (*models.ValidateQuestionResponse, error) {
	resp, err := m.client.ValidateQuestion(ctx, &pbp.ValidateQuestionRequest{
		Options: &pbp.PluginOptions{DryRun: request.DryRun},
		Answer: &pbp.PluginAnswer{
			Question: AdaptQuestionToProto(request.Answer.Question),
			Value:    request.Answer.Value,
		},
	})
	if err != nil {
		return nil, err
	}
	return &models.ValidateQuestionResponse{
		Success: resp.Success,
		Error:   resp.Error,
	}, nil
}

func (m *GRPCClient) DefaultConfig(ctx context.Context, request models.DefaultConfigRequest) (*models.DefaultConfigResponse, error) {
	var answers []*pbp.PluginAnswer
	for _, a := range request.Answers {
		answers = append(answers, &pbp.PluginAnswer{
			Question: AdaptQuestionToProto(a.Question),
			Value:    a.Value,
		})
	}
	resp, err := m.client.DefaultConfig(ctx, &pbp.DefaultConfigRequest{
		Options: &pbp.PluginOptions{DryRun: request.DryRun},
		Answers: answers,
	})
	if err != nil {
		return nil, err
	}
	return &models.DefaultConfigResponse{
		Config: AdaptConfigsFromProto(resp.Config),
	}, nil
}

func (m *GRPCClient) DefaultAssets(ctx context.Context, request models.DefaultAssetsRequest) (*models.DefaultAssetsResponse, error) {
	var answers []*pbp.PluginAnswer
	for _, a := range request.Answers {
		answers = append(answers, &pbp.PluginAnswer{
			Question: AdaptQuestionToProto(a.Question),
			Value:    a.Value,
		})
	}
	resp, err := m.client.DefaultAssets(ctx, &pbp.DefaultAssetsRequest{
		Options: &pbp.PluginOptions{DryRun: request.DryRun},
		Answers: answers,
	})
	if err != nil {
		return nil, err
	}
	return &models.DefaultAssetsResponse{
		Assets: AdaptAssetsFromProto(resp.Assets),
	}, nil
}
