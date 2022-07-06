package playground

import (
	"time"

	"github.com/odpf/optimus/config"
	"github.com/spf13/cobra"
)

type windowComand struct {
	clientConfig         *config.ClientConfig
	window               Window
	size                 int
	offset               int
	truncated            string
	sehduledDate         time.Time
	currentFinishingDate time.Time
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
func (j *windowComand) RunE(_ *cobra.Command, _ []string) error {
	var state string = "Y"
	for state == "Y" {
		j.size = j.window.survey.GetWindowSize()
		j.offset = j.window.survey.GetOffsetSize()
		j.truncated = j.window.survey.GetTrucatedTo()
		j.sehduledDate = j.window.survey.GetSechduleDate()
		j.currentFinishingDate = j.sehduledDate
		j.currentFinishingDate = j.window.truncate(j.currentFinishingDate, j.truncated)
		j.currentFinishingDate = j.window.applyoffset(j.currentFinishingDate, j.offset)
		j.window.printWindow(j.currentFinishingDate, j.size)
		state = j.window.survey.GetStateInput()
	}
	return nil
}
