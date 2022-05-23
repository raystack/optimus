package admin

import (
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
)

type adminCommand struct {
	configFilePath string
	clientConfig   *config.ClientConfig

	rootCommand *cobra.Command
}

// NewAdminCommand initializes command for admin
func NewAdminCommand(rootCmd *cobra.Command) *cobra.Command {
	admin := &adminCommand{
		clientConfig: &config.ClientConfig{},
		rootCommand:  rootCmd,
	}

	cmd := &cobra.Command{
		Use:               "admin",
		Short:             "Internal administration commands",
		Hidden:            true,
		PersistentPreRunE: admin.PersistentPreRunE,
	}
	cmd.PersistentFlags().StringVarP(&admin.configFilePath, "config", "c", admin.configFilePath, "File path for client configuration")

	cmd.AddCommand(NewBuildCommand(admin.clientConfig))
	return cmd
}

func (a *adminCommand) PersistentPreRunE(cmd *cobra.Command, args []string) error {
	a.rootCommand.PersistentPreRun(cmd, args)

	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(a.configFilePath, cmd.Flags())
	if err != nil {
		return err
	}
	*a.clientConfig = *c
	return nil
}
