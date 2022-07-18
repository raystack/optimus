package playground

import (
	"github.com/odpf/optimus/config"
	"github.com/spf13/cobra"
)

type playGroundCommand struct {
	clientConfig *config.ClientConfig
}

// acts a folder for other playground commands
// NewJobCommand initializes command for job
func NewPlayGroundCommand() *cobra.Command {
	playGround := playGroundCommand{
		clientConfig: &config.ClientConfig{},
	}
	cmd := &cobra.Command{
		Use:   "playground",
		Short: "play around with certain functions",
	}
	cmd.AddCommand(NewPlayGroundWindowCommand(playGround.clientConfig))
	return cmd

}
