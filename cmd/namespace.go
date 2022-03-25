package cmd

import (
	"errors"

	"github.com/odpf/optimus/config"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/salt/log"
)

func askToSelectNamespace(l log.Logger, conf config.Optimus) (*config.Namespace, error) {
	options := make([]string, len(conf.Namespaces))
	for i, namespace := range conf.Namespaces {
		options[i] = namespace.Name
	}
	if len(options) == 0 {
		return nil, errors.New("no namespace found in config file")
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
			l.Error("Namespace name cannot be empty")
			continue
		}
		namespace, err := conf.GetNamespaceByName(response)
		if err != nil {
			l.Error(err.Error())
			continue
		}
		return namespace, nil
	}
}
