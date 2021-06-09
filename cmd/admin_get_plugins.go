package cmd

import (
	"context"

	"github.com/odpf/optimus/models"
	cli "github.com/spf13/cobra"
)

func adminGetPluginsCommand(l logger, taskRepo models.TaskPluginRepository, hookRepo models.HookRepo) *cli.Command {
	cmd := &cli.Command{
		Use:     "plugins",
		Short:   "Get discovered plugins",
		Example: `optimus admin get plugins`,
	}

	//TODO: add an option to list all server supported plugins
	cmd.RunE = func(c *cli.Command, args []string) error {
		l.Println("Discovered tasks:")
		for taskIdx, tasks := range taskRepo.GetAll() {
			schema, err := tasks.GetTaskSchema(context.Background(), models.GetTaskSchemaRequest{})
			if err != nil {
				return err
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
				return err
			}
			l.Printf("%d. %s\n", hookIdx+1, schema.Name)
			l.Printf("Description: %s\n", schema.Description)
			l.Printf("Image: %s\n", schema.Image)
			l.Println("")
		}
		return nil
	}
	return cmd
}
