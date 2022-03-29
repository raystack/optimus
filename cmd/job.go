package cmd

import (
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

func jobCommand(l log.Logger, conf config.Optimus, pluginRepo models.PluginRepository) *cli.Command {
	cmd := &cli.Command{
		Use:   "job",
		Short: "Interact with schedulable Job",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.AddCommand(jobCreateCommand(l, conf, pluginRepo))
	cmd.AddCommand(jobAddHookCommand(l, conf, pluginRepo))
	cmd.AddCommand(jobRenderTemplateCommand(l, conf, pluginRepo))
	cmd.AddCommand(jobValidateCommand(l, conf, pluginRepo, conf.Project.Name, conf.Host))
	cmd.AddCommand(jobRunCommand(l, conf, pluginRepo, conf.Project.Name, conf.Host))
	cmd.AddCommand(jobRunListCommand(l, conf.Project.Name, conf.Host))
	return cmd
}
