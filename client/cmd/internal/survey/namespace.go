package survey

import (
	"errors"

	"github.com/AlecAivazis/survey/v2"
	"github.com/raystack/salt/log"

	"github.com/raystack/optimus/config"
)

// NamespaceSurvey defines surveys related to namespace
type NamespaceSurvey struct {
	logger log.Logger
}

// NewNamespaceSurvey initializes namespace survey
func NewNamespaceSurvey(logger log.Logger) *NamespaceSurvey {
	return &NamespaceSurvey{
		logger: logger,
	}
}

// AskToSelectNamespace askesk the user through CLI about namespace to be selected
func (n *NamespaceSurvey) AskToSelectNamespace(clientConfig *config.ClientConfig) (*config.Namespace, error) {
	options := make([]string, len(clientConfig.Namespaces))
	if len(clientConfig.Namespaces) == 0 {
		return nil, errors.New("no namespace found in config file")
	}
	for i, namespace := range clientConfig.Namespaces {
		options[i] = namespace.Name
	}
	prompt := &survey.Select{
		Message: "Please choose the namespace:",
		Options: options,
	}
	for {
		var response string
		if err := survey.AskOne(prompt, &response); err != nil {
			return nil, err
		}
		if response == "" {
			n.logger.Error("Namespace name cannot be empty")
			continue
		}
		namespace, err := clientConfig.GetNamespaceByName(response)
		if err != nil {
			n.logger.Error(err.Error())
			continue
		}
		return namespace, nil
	}
}
