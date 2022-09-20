package cli

import (
	pb "github.com/odpf/optimus/protos/odpf/optimus/plugins/v1beta1"
	"github.com/odpf/optimus/models"
)

func AdaptQuestionToProto(q models.PluginQuestion) *pb.PluginQuestion {
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
				protoSubQ.Questions = append(protoSubQ.Questions, AdaptQuestionToProto(sqq))
			}
			protoSubQuestions = append(protoSubQuestions, protoSubQ)
		}
		pq.SubQuestions = protoSubQuestions
	}
	return pq
}

func AdaptQuestionFromProto(q *pb.PluginQuestion) models.PluginQuestion {
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
				subQ.Questions = append(subQ.Questions, AdaptQuestionFromProto(q))
			}
			pq.SubQuestions = append(pq.SubQuestions, subQ)
		}
	}
	return pq
}

func AdaptConfigsToProto(c models.PluginConfigs) *pb.Configs {
	tc := &pb.Configs{
		Configs: []*pb.Configs_Config{},
	}
	for _, c := range c {
		tc.Configs = append(tc.Configs, &pb.Configs_Config{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}

func AdaptConfigsFromProto(a *pb.Configs) models.PluginConfigs {
	tc := models.PluginConfigs{}
	for _, c := range a.Configs {
		tc = append(tc, models.PluginConfig{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}

func AdaptAssetsToProto(a models.PluginAssets) *pb.Assets {
	tc := &pb.Assets{
		Assets: []*pb.Assets_Asset{},
	}
	for _, c := range a {
		tc.Assets = append(tc.Assets, &pb.Assets_Asset{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}

func AdaptAssetsFromProto(a *pb.Assets) models.PluginAssets {
	tc := models.PluginAssets{}
	for _, c := range a.Assets {
		tc = append(tc, models.PluginAsset{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}
