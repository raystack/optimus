package commands

import (
	"context"

	"github.com/odpf/optimus/models"
	cli "github.com/spf13/cobra"
)

func adminGetPluginsCommand(l logger, taskRepo models.TaskPluginRepository, hookRepo models.HookRepo) *cli.Command {
	cmd := &cli.Command{
		Use:     "plugins",
		Short:   "Get discovered plugins",
		Example: `opctl admin get plugins`,
	}

	//TODO: add an option to list all server supported plugins
	cmd.Run = func(c *cli.Command, args []string) {
		l.Println("Discovered tasks:")
		for taskIdx, tasks := range taskRepo.GetAll() {
			schema, err := tasks.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
			if err != nil {
				errExit(l, err)
			}
			l.Printf("%d. %s\n", taskIdx+1, schema.Name)
			l.Printf("Description: %s\n", schema.Description)
			l.Printf("Image: %s\n", schema.Image)
			l.Println("")
		}

		l.Println("Discovered hooks:")
		for hookIdx, hooks := range hookRepo.GetAll() {
			schema, err := hooks.GetHookSchema(context.Background(), models.GetHookSchemaRequest{})
			if err != nil {
				errExit(l, err)
			}
			l.Printf("%d. %s\n", hookIdx+1, schema.Name)
			l.Printf("Description: %s\n", schema.Description)
			l.Printf("Image: %s\n", schema.Image)
			l.Println("")
		}
	}
	return cmd
}
