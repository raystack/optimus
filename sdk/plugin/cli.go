package plugin

import "context"

// CommandLineMod needs to be implemented by plugins to interact with optimus CLI
type CommandLineMod interface {
	// GetQuestions list down all the cli inputs required to generate spec files
	// name used for question will be directly mapped to DefaultConfig() parameters
	GetQuestions(context.Context, GetQuestionsRequest) (*GetQuestionsResponse, error)
	ValidateQuestion(context.Context, ValidateQuestionRequest) (*ValidateQuestionResponse, error)

	// DefaultConfig will be passed down to execution unit as env vars
	// they will be generated based on results of AskQuestions
	// if DryRun is true in PluginOptions, should not throw error for missing inputs
	DefaultConfig(context.Context, DefaultConfigRequest) (*DefaultConfigResponse, error)

	// DefaultAssets will be passed down to execution unit as files
	// if DryRun is true in PluginOptions, should not throw error for missing inputs
	DefaultAssets(context.Context, DefaultAssetsRequest) (*DefaultAssetsResponse, error)
}

type GetQuestionsRequest struct {
	Options

	JobName string
}

type GetQuestionsResponse struct {
	Questions Questions `yaml:",omitempty"`
}

type ValidateQuestionRequest struct {
	Options

	Answer Answer
}

type ValidateQuestionResponse struct {
	Success bool
	Error   string
}

type DefaultConfigRequest struct {
	Options

	Answers Answers
}

type DefaultConfigResponse struct {
	Config Configs `yaml:"defaultconfig,omitempty"`
}

type DefaultAssetsRequest struct {
	Options

	Answers Answers
}

type DefaultAssetsResponse struct {
	Assets Assets `yaml:"defaultassets,omitempty"`
}
