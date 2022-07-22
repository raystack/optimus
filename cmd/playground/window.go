package playground

import (
	"log"

	"github.com/spf13/cobra"

	tea "github.com/charmbracelet/bubbletea"
)

type windowCommand struct {
	flags int
}

// NewPlaygroundWindowCommand initializes command for window
func NewPlaygroundWindowCommand() *cobra.Command {
	window := windowCommand{flags: 0}
	cmd := &cobra.Command{
		Use:   "window",
		Short: "get dStart,dEnd by giving the window params",
		RunE:  window.RunE,
	}
	return cmd
}

func (j *windowCommand) RunE(_ *cobra.Command, _ []string) error {
	// starts the interactive session
	log.SetFlags(j.flags)
	log.Print("Hello folks! This is an interactive cli")
	log.Println("controls:- use arrow keys to navigate , shift + arrow keys to increment or decrement values")
	log.Println("Size and Offset can be given as input in the form of text")
	log.Println("press control + c or q to quit")
	log.Println("")
	p := tea.NewProgram(initialModel())
	return p.Start()
}
