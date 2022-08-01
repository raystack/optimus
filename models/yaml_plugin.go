package models

import (
	"context"
	"fmt"
	"reflect"
	"regexp"

	"github.com/AlecAivazis/survey/v2"
)

// validatorFactory, name abbreviated so that
// the global implementation can be called 'validatorFactory'
type vFactory struct{}

func (*vFactory) NewFromRegex(re, message string) survey.Validator {
	var regex = regexp.MustCompile(re)
	return func(v interface{}) error {
		k := reflect.ValueOf(v).Kind()
		if k != reflect.String {
			return fmt.Errorf("was expecting a string, got %s", k.String())
		}
		val := v.(string)
		if !regex.Match([]byte(val)) {
			return fmt.Errorf("%s", message)
		}
		return nil
	}
}

var ValidatorFactory = new(vFactory)

type YamlQuestions struct {
	Questions []YamlQuestion
	Index     map[string]YamlQuestion // lookup for validations
}

// Note: Hoping that names in questions don't clash
func (yq *YamlQuestions) ConstructIndex() {
	yq.Index = make(map[string]YamlQuestion)
	for _, quest := range yq.Questions {
		yq.Index[quest.Name] = quest
		if len(quest.SubQuestions) == 0 {
			continue
		}
		for _, subQuests := range quest.SubQuestions {
			for _, subQuest := range subQuests.Questions {
				yq.Index[subQuest.Name] = subQuest
			}
		}
	}
}

func (yq *YamlQuestions) GetQuestionByName(name string) *YamlQuestion {
	if quest, ok := yq.Index[name]; ok {
		return &quest
	}
	return nil
}

// type YamlQuestions []YamlQuestion
type YamlQuestion struct {
	Name         string
	Prompt       string
	Help         string
	Default      string
	Multiselect  []string
	SubQuestions []YamlSubQuestion

	Regexp          string
	ValidationError string
	MinLength       int
	MaxLength       int
	Required        bool
}

func (yq *YamlQuestion) isValid(value string) error {
	if yq.Required {
		return survey.Required(value)
	}
	var validators []survey.Validator
	if yq.Regexp != "" {
		validators = append(validators, ValidatorFactory.NewFromRegex(yq.Regexp, yq.ValidationError))
	}
	if yq.MinLength != 0 {
		validators = append(validators, survey.MinLength(yq.MinLength))
	}
	if yq.MaxLength != 0 {
		validators = append(validators, survey.MaxLength(yq.MaxLength))
	}
	return survey.ComposeValidators(validators...)(value)
}

type YamlSubQuestion struct {
	IfValue   string
	Questions []YamlQuestion
}

type YamlAsset struct {
	DefaultAssets PluginAssets
}

// YamlPlugin: Implements CommandLineMod
type YamlPlugin struct {
	Info            *PluginInfoResponse
	YamlQuestions   *YamlQuestions
	PluginQuestions *GetQuestionsResponse
	PluginAssets    *YamlAsset
}

func (p *YamlPlugin) PluginInfo() (*PluginInfoResponse, error) {
	return &PluginInfoResponse{
		Name:          p.Info.Name,
		Description:   p.Info.Description,
		Image:         p.Info.Image,
		DependsOn:     p.Info.DependsOn,
		PluginType:    p.Info.PluginType,
		PluginVersion: p.Info.PluginVersion,
		HookType:      p.Info.HookType,
		PluginMods:    p.Info.PluginMods,
	}, nil
}

func (p *YamlPlugin) GetQuestions(context.Context, GetQuestionsRequest) (*GetQuestionsResponse, error) {
	return p.PluginQuestions, nil
}

func (p *YamlPlugin) ValidateQuestion(_ context.Context, req ValidateQuestionRequest) (*ValidateQuestionResponse, error) {
	question := req.Answer.Question
	value := req.Answer.Value
	yamlQuestion := p.YamlQuestions.GetQuestionByName(question.Name)
	if err := yamlQuestion.isValid(value); err != nil {
		return &ValidateQuestionResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	return &ValidateQuestionResponse{
		Success: true,
	}, nil
}

func (*YamlPlugin) DefaultConfig(_ context.Context, req DefaultConfigRequest) (*DefaultConfigResponse, error) {
	conf := []PluginConfig{}
	for _, ans := range req.Answers {
		conf = append(conf, PluginConfig{
			Name:  ans.Question.Name,
			Value: ans.Value,
		})
	}
	return &DefaultConfigResponse{
		Config: conf,
	}, nil
}

// just to be compatible with CLIMOD Interface
func (p *YamlPlugin) DefaultAssets(context.Context, DefaultAssetsRequest) (*DefaultAssetsResponse, error) {
	return &DefaultAssetsResponse{
		Assets: p.PluginAssets.DefaultAssets,
	}, nil
}

// just to be compatible with CLIMOD Interface
func (*YamlPlugin) CompileAssets(context.Context, CompileAssetsRequest) (*CompileAssetsResponse, error) {
	return nil, nil // nolint:nilnil
}
