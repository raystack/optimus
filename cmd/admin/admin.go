package admin

import (
	"github.com/odpf/optimus/config"
	"github.com/spf13/cobra"
)

type adminCommand struct {
	configFilePath string
	clientConfig   *config.ClientConfig
}

// NewAdminCommand initializes command for admin
func NewAdminCommand() *cobra.Command {
	admin := &adminCommand{}

	cmd := &cobra.Command{
		Use:    "admin",
		Short:  "Internal administration commands",
		Hidden: true,
	}
	cmd.PersistentFlags().StringVarP(&admin.configFilePath, "config", "c", admin.configFilePath, "File path for client configuration")
	cmd.AddCommand(NewBuildCommand(admin.clientConfig))
	return cmd
}

func (a *adminCommand) PersistentPreRunE(cmd *cobra.Command, args []string) error {
	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(a.configFilePath, cmd.Flags())
	if err != nil {
		return err
	}
	*a.clientConfig = *c
	return nil
}
