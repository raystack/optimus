package cli

import (
	"context"
	"strings"

	"github.com/odpf/optimus/plugin/base"

	"github.com/odpf/optimus/models"

	pbp "github.com/odpf/optimus/api/proto/odpf/optimus/plugins"
)

// GRPCServer will be used by plugins this is working as proto adapter
type GRPCServer struct {
	pbp.UnimplementedCLIModServer

	// This is the real implementation coming from plugin
	Impl models.CommandLineMod

	baseClient *base.GRPCClient
}

func (s *GRPCServer) PluginInfo() (*models.PluginInfoResponse, error) {
	return s.baseClient.PluginInfo()
}

func (s *GRPCServer) GetQuestions(ctx context.Context, req *pbp.GetQuestionsRequest) (*pbp.GetQuestionsResponse, error) {
	resp, err := s.Impl.GetQuestions(ctx, models.GetQuestionsRequest{
		JobName:       req.JobName,
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	questions := []*pbp.PluginQuestion{}
	for _, q := range resp.Questions {
		questions = append(questions, AdaptQuestionToProto(q))
	}
	return &pbp.GetQuestionsResponse{Questions: questions}, nil
}

func (s *GRPCServer) ValidateQuestion(ctx context.Context, req *pbp.ValidateQuestionRequest) (*pbp.ValidateQuestionResponse, error) {
	resp, err := s.Impl.ValidateQuestion(ctx, models.ValidateQuestionRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
		Answer: models.PluginAnswer{
			Question: AdaptQuestionFromProto(req.Answer.Question),
			Value:    req.Answer.Value,
		},
	})
	if err != nil {
		return nil, err
	}
	return &pbp.ValidateQuestionResponse{
		Success: resp.Success,
		Error:   resp.Error,
	}, nil
}

func (s *GRPCServer) DefaultConfig(ctx context.Context, req *pbp.DefaultConfigRequest) (*pbp.DefaultConfigResponse, error) {
	answers := models.PluginAnswers{}
	for _, ans := range req.Answers {
		answers = append(answers, models.PluginAnswer{
			Question: AdaptQuestionFromProto(ans.Question),
			Value:    ans.Value,
		})
	}
	resp, err := s.Impl.DefaultConfig(ctx, models.DefaultConfigRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
		Answers:       answers,
	})
	if err != nil {
		return nil, err
	}
	return &pbp.DefaultConfigResponse{
		Config: AdaptConfigsToProto(resp.Config),
	}, nil
}

func (s *GRPCServer) DefaultAssets(ctx context.Context, req *pbp.DefaultAssetsRequest) (*pbp.DefaultAssetsResponse, error) {
	answers := models.PluginAnswers{}
	for _, ans := range req.Answers {
		answers = append(answers, models.PluginAnswer{
			Question: AdaptQuestionFromProto(ans.Question),
			Value:    ans.Value,
		})
	}
	resp, err := s.Impl.DefaultAssets(ctx, models.DefaultAssetsRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
		Answers:       answers,
	})
	if err != nil {
		return nil, err
	}
	return &pbp.DefaultAssetsResponse{
		Assets: AdaptAssetsToProto(resp.Assets),
	}, nil
}

func (s *GRPCServer) CompileAssets(ctx context.Context, req *pbp.CompileAssetsRequest) (*pbp.CompileAssetsResponse, error) {
	var instanceData []models.InstanceSpecData
	for _, inst := range req.InstanceData {
		instanceData = append(instanceData, models.InstanceSpecData{
			Name:  inst.Name,
			Value: inst.Value,
			Type:  strings.ToLower(inst.Type.String()),
		})
	}

	resp, err := s.Impl.CompileAssets(ctx, models.CompileAssetsRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
		Config:        AdaptConfigsFromProto(req.Configs),
		Assets:        AdaptAssetsFromProto(req.Assets),
		Window: models.JobSpecTaskWindow{
			Size:       req.Window.Size.AsDuration(),
			Offset:     req.Window.Offset.AsDuration(),
			TruncateTo: req.Window.TruncateTo,
		},
		InstanceData:     instanceData,
		InstanceSchedule: req.InstanceSchedule.AsTime(),
	})
	if err != nil {
		return nil, err
	}
	return &pbp.CompileAssetsResponse{
		Assets: AdaptAssetsToProto(resp.Assets),
	}, nil
}
