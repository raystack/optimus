package cmd

import (
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
)

func jobCommand(l log.Logger, pluginRepo models.PluginRepository) *cli.Command {
	cmd := &cli.Command{
		Use:   "job",
		Short: "Interact with schedulable Job",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	// TODO: find a way to load the config in one place
	conf, err := config.LoadClientConfig()
	if err != nil {
		l.Error(err.Error())
		return nil
	}

	cmd.AddCommand(jobCreateCommand(l, *conf, pluginRepo))
	cmd.AddCommand(jobAddHookCommand(l, *conf, pluginRepo))
	cmd.AddCommand(jobRenderTemplateCommand(l, *conf, pluginRepo))
	cmd.AddCommand(jobValidateCommand(l, *conf, pluginRepo))
	cmd.AddCommand(jobRunCommand(l, *conf, pluginRepo))
	cmd.AddCommand(jobRunListCommand(l, conf.Project.Name, conf.Host))
	return cmd
}
