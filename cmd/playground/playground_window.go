package playground

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/odpf/optimus/config"
	"github.com/spf13/cobra"
)

type windowComand struct {
	clientConfig *config.ClientConfig
}

// NewPlayGroundWindowCommand initializes command for window
func NewPlayGroundWindowCommand(clientConfig *config.ClientConfig) *cobra.Command {
	windowComand := windowComand{
		clientConfig: &config.ClientConfig{},
	}
	cmd := &cobra.Command{
		Use:   "window",
		Short: "get dStart,dEnd by giving the window params",
		RunE:  windowComand.RunE,
	}
	return cmd
}

// this will run when we give the command in cli
func (j *windowComand) RunE(_ *cobra.Command, _ []string) error {
	// starts the interactive session
	fmt.Println(">  hello folks! this is an interactive cli")
	fmt.Println(">  controls use arrow keys to navigate , shift + arrow keys to increment or decrement values")
	fmt.Println(">  press control + c or q to quit")
	fmt.Println("") // to maintain space for asthetic reasons
	p := tea.NewProgram(initialModel())
	p.Start()
	return nil
}
