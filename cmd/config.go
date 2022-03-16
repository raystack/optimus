package cmd

import (
	"fmt"
	"io/ioutil"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	"github.com/odpf/optimus/config"
)

const (
	defaultHost = "localhost"
)

func configCommand(l log.Logger) *cli.Command {
	c := &cli.Command{
		Use:   "config",
		Short: "Manage optimus configuration required to deploy specifications",
	}
	c.AddCommand(configInitCommand(l))
	return c
}

func configInitCommand(l log.Logger) *cli.Command {
	c := &cli.Command{
		Use:   "init",
		Short: "Initialize optimus configuration file",
		RunE: func(c *cli.Command, args []string) (err error) {
			conf := config.Optimus{
				Version: 1,
				Host:    defaultHost,
			}
			questions := []*survey.Question{
				{
					Name: "ProjectName",
					Prompt: &survey.Input{
						Message: "What is the project name?",
						Help:    "Project name of the optimus repository",
					},
					Validate: survey.Required,
				},
				{
					Name: "RegisterProjectConfig",
					Prompt: &survey.Select{
						Message: "Register project configs?",
						Options: []string{AnswerYes, AnswerNo},
						Default: AnswerNo,
					},
				},
			}
			answers := map[string]interface{}{}
			if err := survey.Ask(questions, &answers); err != nil {
				return err
			}
			conf.Project.Name = answers["ProjectName"].(string)

			// for project config
			if option, ok := answers["RegisterProjectConfig"]; ok && option.(survey.OptionAnswer).Value == AnswerYes {
				conf, err = projectConfigQuestions(conf)
				if err != nil {
					return err
				}
			}

			// questions for namespace config
			questions = []*survey.Question{
				{
					Name: "NamespaceName",
					Prompt: &survey.Input{
						Message: "What is the namespace name?",
						Help:    "Namespace name for jobs and resources inside the directory",
					},
					Validate: survey.Required,
				},
				{
					Name: "RegisterNamespaceConfig",
					Prompt: &survey.Select{
						Message: "Register namespace configs?",
						Options: []string{AnswerYes, AnswerNo},
						Default: AnswerNo,
					},
				},
			}
			answers = map[string]interface{}{}
			if err := survey.Ask(questions, &answers); err != nil {
				return err
			}
			confMarshaled, err := yaml.Marshal(conf)
			if err != nil {
				return err
			}
			if err := ioutil.WriteFile(fmt.Sprintf("%s.%s", config.FileName, config.FileExtension), confMarshaled, 0o655); err != nil {
				return err
			}
			l.Info(coloredSuccess("Configuration initialised successfully"))
			return nil
		},
	}
	return c
}

func projectConfigQuestions(conf config.Optimus) (config.Optimus, error) {
	conf.Project.Config = map[string]string{}
	registerMore := AnswerYes
	for registerMore == AnswerNo {
		configAnswers := map[string]interface{}{}
		if err := survey.Ask([]*survey.Question{
			{
				Name: "Name",
				Prompt: &survey.Input{
					Message: "Name of the config",
				},
				Validate: survey.MinLength(3),
			},
			{
				Name: "Value",
				Prompt: &survey.Input{
					Message: "Value",
				},
				Validate: survey.MinLength(1),
			},
		}, &configAnswers); err != nil {
			return conf, err
		}

		if err := survey.AskOne(&survey.Select{
			Message: "Add one more?",
			Options: []string{AnswerYes, AnswerNo},
			Default: AnswerYes,
		}, &registerMore); err != nil {
			return conf, err
		}
		conf.Project.Config[configAnswers["Name"].(string)] = configAnswers["Value"].(string)
	}

	return conf, nil
}
