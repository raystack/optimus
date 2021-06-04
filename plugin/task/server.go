package task

import (
	"context"
	"strings"

	"github.com/odpf/optimus/models"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
)

// GRPCServer will be used by plugins this is working as proto adapter
type GRPCServer struct {
	// This is the real implementation coming from plugin
	Impl models.TaskPlugin

	projectSpecAdapter ProjectSpecAdapter
	pb.UnimplementedTaskPluginServer
}

func (s *GRPCServer) GetTaskSchema(ctx context.Context, req *pb.GetTaskSchema_Request) (*pb.GetTaskSchema_Response, error) {
	n, err := s.Impl.GetTaskSchema(ctx, models.GetTaskSchemaRequest{})
	if err != nil {
		return nil, err
	}
	return &pb.GetTaskSchema_Response{
		Name:        n.Name,
		Description: n.Description,
		Image:       n.Image,
		SecretPath:  n.SecretPath,
	}, nil
}

func (s *GRPCServer) GetTaskQuestions(ctx context.Context, req *pb.GetTaskQuestions_Request) (*pb.GetTaskQuestions_Response, error) {
	resp, err := s.Impl.GetTaskQuestions(ctx, models.GetTaskQuestionsRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	questions := []*pb.PluginQuestion{}
	for _, q := range resp.Questions {
		questions = append(questions, adaptQuestionToProto(q))
	}
	return &pb.GetTaskQuestions_Response{Questions: questions}, nil
}

func (s *GRPCServer) ValidateTaskQuestion(ctx context.Context, req *pb.ValidateTaskQuestion_Request) (*pb.ValidateTaskQuestion_Response, error) {
	resp, err := s.Impl.ValidateTaskQuestion(ctx, models.ValidateTaskQuestionRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
		Answer: models.PluginAnswer{
			Question: adaptQuestionFromProto(req.Answer.Question),
			Value:    req.Answer.Value,
		},
	})
	if err != nil {
		return nil, err
	}
	return &pb.ValidateTaskQuestion_Response{
		Success: resp.Success,
		Error:   resp.Error,
	}, nil
}

func (s *GRPCServer) DefaultTaskConfig(ctx context.Context, req *pb.DefaultTaskConfig_Request) (*pb.DefaultTaskConfig_Response, error) {
	answers := models.PluginAnswers{}
	for _, ans := range req.Answers {
		answers = append(answers, models.PluginAnswer{
			Question: adaptQuestionFromProto(ans.Question),
			Value:    ans.Value,
		})
	}
	resp, err := s.Impl.DefaultTaskConfig(ctx, models.DefaultTaskConfigRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
		Answers:       answers,
	})
	if err != nil {
		return nil, err
	}
	return &pb.DefaultTaskConfig_Response{
		Configs: AdaptConfigsToProto(resp.Config),
	}, nil
}

func (s *GRPCServer) DefaultTaskAssets(ctx context.Context, req *pb.DefaultTaskAssets_Request) (*pb.DefaultTaskAssets_Response, error) {
	answers := models.PluginAnswers{}
	for _, ans := range req.Answers {
		answers = append(answers, models.PluginAnswer{
			Question: adaptQuestionFromProto(ans.Question),
			Value:    ans.Value,
		})
	}
	resp, err := s.Impl.DefaultTaskAssets(ctx, models.DefaultTaskAssetsRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
		Answers:       answers,
	})
	if err != nil {
		return nil, err
	}
	return &pb.DefaultTaskAssets_Response{
		Assets: adaptAssetsToProto(resp.Assets),
	}, nil
}

func (s *GRPCServer) CompileTaskAssets(ctx context.Context, req *pb.CompileTaskAssets_Request) (*pb.CompileTaskAssets_Response, error) {
	instanceData := []models.InstanceSpecData{}
	for _, inst := range req.InstanceData {
		instanceData = append(instanceData, models.InstanceSpecData{
			Name:  inst.Name,
			Value: inst.Value,
			Type:  strings.ToLower(inst.Type.String()),
		})
	}
	resp, err := s.Impl.CompileTaskAssets(ctx, models.CompileTaskAssetsRequest{
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
		Config:        AdaptConfigsFromProto(req.JobConfigs),
		Assets:        adaptAssetsFromProto(req.JobAssets),
		TaskWindow: models.JobSpecTaskWindow{
			Size:       req.TaskWindow.Size.AsDuration(),
			Offset:     req.TaskWindow.Offset.AsDuration(),
			TruncateTo: req.TaskWindow.TruncateTo,
		},
		InstanceData:     instanceData,
		InstanceSchedule: req.InstanceSchedule.AsTime(),
	})
	if err != nil {
		return nil, err
	}
	return &pb.CompileTaskAssets_Response{
		Assets: adaptAssetsToProto(resp.Assets),
	}, nil
}

func (s *GRPCServer) GenerateTaskDestination(ctx context.Context, req *pb.GenerateTaskDestination_Request) (*pb.GenerateTaskDestination_Response, error) {
	resp, err := s.Impl.GenerateTaskDestination(ctx, models.GenerateTaskDestinationRequest{
		Config:        AdaptConfigsFromProto(req.JobConfig),
		Assets:        adaptAssetsFromProto(req.JobAssets),
		Project:       s.projectSpecAdapter.FromProjectProtoWithSecrets(req.Project),
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	return &pb.GenerateTaskDestination_Response{Destination: resp.Destination}, nil
}

func (s *GRPCServer) GenerateTaskDependencies(ctx context.Context, req *pb.GenerateTaskDependencies_Request) (*pb.GenerateTaskDependencies_Response, error) {
	resp, err := s.Impl.GenerateTaskDependencies(ctx, models.GenerateTaskDependenciesRequest{
		Config:        AdaptConfigsFromProto(req.JobConfig),
		Assets:        adaptAssetsFromProto(req.JobAssets),
		Project:       s.projectSpecAdapter.FromProjectProtoWithSecrets(req.Project),
		PluginOptions: models.PluginOptions{DryRun: req.Options.DryRun},
	})
	if err != nil {
		return nil, err
	}
	return &pb.GenerateTaskDependencies_Response{Dependencies: resp.Dependencies}, nil
}
