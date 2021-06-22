package hook

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

func adaptConfigsToProto(c models.HookPluginConfigs) *pb.HookConfigs {
	tc := &pb.HookConfigs{
		Configs: []*pb.HookConfigs_Config{},
	}
	for _, c := range c {
		tc.Configs = append(tc.Configs, &pb.HookConfigs_Config{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}

func adaptConfigFromProto(a *pb.HookConfigs) models.HookPluginConfigs {
	tc := models.HookPluginConfigs{}
	for _, c := range a.Configs {
		tc = append(tc, models.HookPluginConfig{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}

func adaptAssetsToProto(a models.HookPluginAssets) *pb.HookAssets {
	tc := &pb.HookAssets{
		Assets: []*pb.HookAssets_Asset{},
	}
	for _, c := range a {
		tc.Assets = append(tc.Assets, &pb.HookAssets_Asset{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}

func adaptAssetsFromProto(a *pb.HookAssets) models.HookPluginAssets {
	tc := models.HookPluginAssets{}
	for _, c := range a.Assets {
		tc = append(tc, models.HookPluginAsset{
			Name:  c.Name,
			Value: c.Value,
		})
	}
	return tc
}
