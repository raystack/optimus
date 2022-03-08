package cmd

import (
	"fmt"
	"os"

	"github.com/odpf/optimus/config"

	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
)

func jobCommand(l log.Logger, conf config.Optimus, pluginRepo models.PluginRepository) *cli.Command {
	cmd := &cli.Command{
		Use:   "job",
		Short: "Interact with schedulable Job",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	var namespaceName string
	cmd.PersistentFlags().StringVarP(&namespaceName, "namespace", "n", "", "targetted namespace for job creation")
	cmd.MarkPersistentFlagRequired("namespace")

	if conf.Namespaces[namespaceName] == nil {
		fmt.Printf("namespace [%s] is not found\n", namespaceName)
		os.Exit(1)
	}
	if namespace := conf.Namespaces[namespaceName]; namespace.Job.Path != "" {
		cmd.AddCommand(jobCreateCommand(l, namespace, pluginRepo))
		cmd.AddCommand(jobAddHookCommand(l, namespace, pluginRepo))
		cmd.AddCommand(jobRenderTemplateCommand(l, namespace, pluginRepo))
		cmd.AddCommand(jobValidateCommand(l, namespace, pluginRepo, conf.Project.Name, conf.Host))
		cmd.AddCommand(jobRunCommand(l, namespace, pluginRepo, conf.Project.Name, conf.Host))
	}
	cmd.AddCommand(jobStatusCommand(l, conf.Project.Name, conf.Host))
	return cmd
}
