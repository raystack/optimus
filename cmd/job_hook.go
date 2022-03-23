package cmd

import (
	"context"
	"errors"
	"fmt"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
	"github.com/odpf/optimus/utils"
	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	cli "github.com/spf13/cobra"
)

func jobAddHookCommand(l log.Logger, conf config.ProjectConfig, pluginRepo models.PluginRepository) *cli.Command {
	var namespaceName string
	cmd := &cli.Command{
		Use:     "addhook",
		Aliases: []string{"add_hook", "add-hook", "addHook", "attach_hook", "attach-hook", "attachHook"},
		Short:   "Attach a new Hook to existing job",
		Long:    "Add a runnable instance that will be triggered before or after the base transformation.",
		Example: "optimus addhook",
		RunE: func(cmd *cli.Command, args []string) error {
			namespace, err := conf.GetNamespaceByName(namespaceName)
			if err != nil {
				return err
			}
			jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), namespace.Job.Path)
			jobSpecRepo := local.NewJobSpecRepository(
				jobSpecFs,
				local.NewJobSpecAdapter(pluginRepo),
			)
			selectJobName, err := selectJobSurvey(jobSpecRepo)
			if err != nil {
				return err
			}
			jobSpec, err := jobSpecRepo.GetByName(selectJobName)
			if err != nil {
				return err
			}
			jobSpec, err = createHookSurvey(jobSpec, pluginRepo)
			if err != nil {
				return err
			}
			if err := jobSpecRepo.Save(jobSpec); err != nil {
				return err
			}
			l.Info(coloredSuccess("Hook successfully added to %s", selectJobName))
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespaceName, "namespace", "n", namespaceName, "targeted namespace for adding hook")
	cmd.MarkFlagRequired("namespace")
	return cmd
}

func createHookSurvey(jobSpec models.JobSpec, pluginRepo models.PluginRepository) (models.JobSpec, error) {
	emptyJobSpec := models.JobSpec{}
	var availableHooks []string
	for _, hook := range pluginRepo.GetHooks() {
		availableHooks = append(availableHooks, hook.Info().Name)
	}
	if len(availableHooks) == 0 {
		return emptyJobSpec, errors.New("no supported hook plugin found")
	}

	var qs = []*survey.Question{
		{
			Name: "hook",
			Prompt: &survey.Select{
				Message: "Select hook to attach?",
				Options: availableHooks,
			},
			Validate: survey.Required,
		},
	}
	baseInputsRaw := make(map[string]interface{})
	if err := survey.Ask(qs, &baseInputsRaw); err != nil {
		return emptyJobSpec, err
	}
	baseInputs, err := utils.ConvertToStringMap(baseInputsRaw)
	if err != nil {
		return emptyJobSpec, err
	}

	selectedHook := baseInputs["hook"]
	if ifHookAlreadyExistsForJob(jobSpec, selectedHook) {
		return emptyJobSpec, fmt.Errorf("hook %s already exists for this job", selectedHook)
	}

	executionHook, err := pluginRepo.GetByName(selectedHook)
	if err != nil {
		return emptyJobSpec, err
	}

	var jobSpecConfigs models.JobSpecConfigs
	cliMod := executionHook.CLIMod
	if cliMod != nil {
		taskQuesResponse, err := cliMod.GetQuestions(context.Background(), models.GetQuestionsRequest{
			JobName: jobSpec.Name,
		})
		if err != nil {
			return emptyJobSpec, err
		}

		userInputs := models.PluginAnswers{}
		if taskQuesResponse.Questions != nil {
			for _, ques := range taskQuesResponse.Questions {
				responseAnswer, err := AskCLISurveyQuestion(ques, cliMod)
				if err != nil {
					return emptyJobSpec, err
				}
				userInputs = append(userInputs, responseAnswer...)
			}
		}

		generateConfResponse, err := cliMod.DefaultConfig(context.Background(), models.DefaultConfigRequest{
			Answers: userInputs,
		})
		if err != nil {
			return emptyJobSpec, err
		}
		if generateConfResponse.Config != nil {
			jobSpecConfigs = generateConfResponse.Config.ToJobSpec()
		}
	}
	jobSpec.Hooks = append(jobSpec.Hooks, models.JobSpecHook{
		Unit:   executionHook,
		Config: jobSpecConfigs,
	})
	return jobSpec, nil
}

func ifHookAlreadyExistsForJob(jobSpec models.JobSpec, newHookName string) bool {
	for _, hook := range jobSpec.Hooks {
		if hook.Unit.Info().Name == newHookName {
			return true
		}
	}
	return false
}
