package hook

import (
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/models"
)

func adaptQuestionToProto(q models.PluginQuestion) *pb.PluginQuestion {
	pq := &pb.PluginQuestion{
		Name:                q.Name,
		Prompt:              q.Prompt,
		Help:                q.Help,
		Default:             q.Default,
		Multiselect:         q.Multiselect,
		SubQuestionsIfValue: q.SubQuestionsIfValue,
	}
	subQ := []*pb.PluginQuestion{}
	if len(q.SubQuestions) > 0 {
		for _, sq := range q.SubQuestions {
			subQ = append(subQ, adaptQuestionToProto(sq))
		}
		pq.SubQuestions = subQ
	}
	return pq
}

func adaptQuestionFromProto(q *pb.PluginQuestion) models.PluginQuestion {
	pq := models.PluginQuestion{
		Name:                q.Name,
		Prompt:              q.Prompt,
		Help:                q.Help,
		Default:             q.Default,
		Multiselect:         q.Multiselect,
		SubQuestionsIfValue: q.SubQuestionsIfValue,
	}
	subQ := models.PluginQuestions{}
	if len(q.SubQuestions) > 0 {
		for _, sq := range q.SubQuestions {
			subQ = append(subQ, adaptQuestionFromProto(sq))
		}
		pq.SubQuestions = subQ
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
