package job

import (
	"fmt"

	"github.com/spf13/cobra"
)

type explainCommand struct {
}

func NewExplainCommand() *cobra.Command {
	explain := &explainCommand{}
	cmd := &cobra.Command{
		Use:   "explain",
		Short: "gets job dependencies and ",
		Long:  "Process optimus job specification based on macros/functions used.",
		RunE:  explain.RunE,
	}
	return cmd
}
func (e *explainCommand) RunE(_ *cobra.Command, _ []string) error {
	fmt.Println("explain command is run")
	return nil
}
