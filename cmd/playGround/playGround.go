package playground

import (
	"github.com/spf13/cobra"
	"github.com/odpf/optimus/config"
)

type playGroundCommand struct {
	clientConfig *config.ClientConfig
}

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
