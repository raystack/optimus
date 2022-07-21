package playground

import (
	"fmt"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/spf13/cobra"
)

type windowCommand struct {
}

// NewPlaygroundWindowCommand initializes command for window
func NewPlaygroundWindowCommand() *cobra.Command {
	window := windowCommand{}
	cmd := &cobra.Command{
		Use:   "window",
		Short: "get dStart,dEnd by giving the window params",
		RunE:  window.RunE,
	}
	return cmd
}

func (j *windowCommand) RunE(_ *cobra.Command, _ []string) error {
	// starts the interactive session
	fmt.Println("Hello folks! This is an interactive cli")
	fmt.Println("controls:- use arrow keys to navigate , shift + arrow keys to increment or decrement values")
	fmt.Println("Size and Offset can be given as input in the form of text")
	fmt.Println("press control + c or q to quit")
	fmt.Println("")
	p := tea.NewProgram(initialModel())
	return p.Start()
}
