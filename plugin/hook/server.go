package hook

import (
	"context"

	"github.com/odpf/optimus/plugin/task"

	"github.com/odpf/optimus/models"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
)

// GRPCServer will be used by plugins this is working as proto adapter
type GRPCServer struct {
	// This is the real implementation
	Impl models.HookPlugin

	projectSpecAdapter ProjectSpecAdapter
	pb.UnimplementedHookPluginServer
}

func (s *GRPCServer) GetHookSchema(ctx context.Context, req *pb.GetHookSchema_Request) (*pb.GetHookSchema_Response, error) {
	n, err := s.Impl.GetHookSchema(ctx, models.GetHookSchemaRequest{})
	if err != nil {
		return nil, err
	}
	return &pb.GetHookSchema_Response{
		Name:        n.Name,
		Description: n.Description,
		Image:       n.Image,
		Type:        n.Type.String(),
		DependsOn:   n.DependsOn,
		SecretPath:  n.SecretPath,
	}, nil
}

func (s *GRPCServer) GetHookQuestions(ctx context.Context, req *pb.GetHookQuestions_Request) (*pb.GetHookQuestions_Response, error) {
	resp, err := s.Impl.GetHookQuestions(ctx, models.GetHookQuestionsRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	questions := []*pb.PluginQuestion{}
	for _, q := range resp.Questions {
		questions = append(questions, adaptQuestionToProto(q))
	}
	return &pb.GetHookQuestions_Response{Questions: questions}, nil
}

func (s *GRPCServer) ValidateHookQuestion(ctx context.Context, req *pb.ValidateHookQuestion_Request) (*pb.ValidateHookQuestion_Response, error) {
	resp, err := s.Impl.ValidateHookQuestion(ctx, models.ValidateHookQuestionRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
		Answer: models.PluginAnswer{
			Question: adaptQuestionFromProto(req.Answer.Question),
			Value:    req.Answer.Value,
		},
	})
	if err != nil {
		return nil, err
	}
	return &pb.ValidateHookQuestion_Response{
		Success: resp.Success,
		Error:   resp.Error,
	}, nil
}

func (s *GRPCServer) DefaultHookConfig(ctx context.Context, req *pb.DefaultHookConfig_Request) (*pb.DefaultHookConfig_Response, error) {
	answers := models.PluginAnswers{}
	for _, ans := range req.Answers {
		answers = append(answers, models.PluginAnswer{
			Question: adaptQuestionFromProto(ans.Question),
			Value:    ans.Value,
		})
	}
	resp, err := s.Impl.DefaultHookConfig(ctx, models.DefaultHookConfigRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
		Answers:       answers,
		TaskConfig:    task.AdaptConfigsFromProto(req.TaskConfigs),
	})
	if err != nil {
		return nil, err
	}
	return &pb.DefaultHookConfig_Response{
		Config: adaptConfigsToProto(resp.Config),
	}, nil
}

func (s *GRPCServer) DefaultHookAssets(ctx context.Context, req *pb.DefaultHookAssets_Request) (*pb.DefaultHookAssets_Response, error) {
	answers := models.PluginAnswers{}
	for _, ans := range req.Answers {
		answers = append(answers, models.PluginAnswer{
			Question: adaptQuestionFromProto(ans.Question),
			Value:    ans.Value,
		})
	}
	resp, err := s.Impl.DefaultHookAssets(ctx, models.DefaultHookAssetsRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
		Answers:       answers,
		TaskConfig:    task.AdaptConfigsFromProto(req.TaskConfigs),
	})
	if err != nil {
		return nil, err
	}
	return &pb.DefaultHookAssets_Response{
		Assets: adaptAssetsToProto(resp.Assets),
	}, nil
}
