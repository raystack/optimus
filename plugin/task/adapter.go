package task

import (
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/models"
)

func adaptQuestionToProto(q models.PluginQuestion) *pb.PluginQuestion {
	pq := &pb.PluginQuestion{
		Name:        q.Name,
		Prompt:      q.Prompt,
		Help:        q.Help,
		Default:     q.Default,
		Multiselect: q.Multiselect,
	}
	var protoSubQuestions []*pb.PluginQuestion_SubQuestion
	if len(q.SubQuestions) > 0 {
		for _, sq := range q.SubQuestions {
			protoSubQ := &pb.PluginQuestion_SubQuestion{
				IfValue:   sq.IfValue,
				Questions: []*pb.PluginQuestion{},
			}
			for _, sqq := range sq.Questions {
				protoSubQ.Questions = append(protoSubQ.Questions, adaptQuestionToProto(sqq))
			}
			protoSubQuestions = append(protoSubQuestions, protoSubQ)
		}
		pq.SubQuestions = protoSubQuestions
	}
	return pq
}

func adaptQuestionFromProto(q *pb.PluginQuestion) models.PluginQuestion {
	pq := models.PluginQuestion{
		Name:         q.Name,
		Prompt:       q.Prompt,
		Help:         q.Help,
		Default:      q.Default,
		Multiselect:  q.Multiselect,
		SubQuestions: []models.PluginSubQuestion{},
	}
	if len(q.SubQuestions) > 0 {
		for _, protoSubQ := range q.SubQuestions {
			subQ := models.PluginSubQuestion{
				IfValue:   protoSubQ.IfValue,
				Questions: models.PluginQuestions{},
			}
			for _, q := range protoSubQ.Questions {
				subQ.Questions = append(subQ.Questions, adaptQuestionFromProto(q))
			}
			pq.SubQuestions = append(pq.SubQuestions, subQ)
		}
	}
	return pq
}

func AdaptConfigsToProto(c models.TaskPluginConfigs) *pb.TaskConfigs {
	tc := &pb.TaskConfigs{
		Configs: []*pb.TaskConfigs_Config{},
	}
	for _, c := range c {
		tc.Configs = append(tc.Configs, &pb.TaskConfigs_Config{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}

func AdaptConfigsFromProto(a *pb.TaskConfigs) models.TaskPluginConfigs {
	tc := models.TaskPluginConfigs{}
	for _, c := range a.Configs {
		tc = append(tc, models.TaskPluginConfig{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}

func adaptAssetsToProto(a models.TaskPluginAssets) *pb.TaskAssets {
	tc := &pb.TaskAssets{
		Assets: []*pb.TaskAssets_Asset{},
	}
	for _, c := range a {
		tc.Assets = append(tc.Assets, &pb.TaskAssets_Asset{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}

func adaptAssetsFromProto(a *pb.TaskAssets) models.TaskPluginAssets {
	tc := models.TaskPluginAssets{}
	for _, c := range a.Assets {
		tc = append(tc, models.TaskPluginAsset{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}
