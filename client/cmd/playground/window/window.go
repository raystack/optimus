package window

import (
	tea "github.com/charmbracelet/bubbletea"
	"github.com/raystack/salt/log"
	"github.com/spf13/cobra"

	"github.com/raystack/optimus/client/cmd/internal/logger"
)

type command struct {
	log log.Logger
}

// NewCommand initializes command for window playground
func NewCommand() *cobra.Command {
	window := command{log: logger.NewClientLogger()}
	cmd := &cobra.Command{
		Use:   "window",
		Short: "Play around with window configuration",
		RunE:  window.RunE,
	}
	return cmd
}

func (j *command) RunE(_ *cobra.Command, _ []string) error {
	j.log.Info("Hi, this is an interactive CLI to play around with window configuration.")
	j.log.Info("Navigate around the available configurations input with arrow keys.")
	j.log.Info("If you want to quit, just press 'q' or 'ctr+c' key.\n")
	p := tea.NewProgram(newModel())
	return p.Start()
}
