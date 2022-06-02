package survey

import (
	"fmt"

	"github.com/AlecAivazis/survey/v2"
)

// ExtensionSurvey defines survey for extension
type ExtensionSurvey struct{}

// NewExtensionSurvey initializes extension survey
func NewExtensionSurvey() *ExtensionSurvey {
	return nil
}

// AskConfirmClean asks the user to confirm clean
func (*ExtensionSurvey) AskConfirmClean() (bool, error) {
	var output bool
	prompt := &survey.Confirm{
		Message: "Do you want to clean all extensions from local?",
		Help:    "This operation removes all extensions and its manifest from local.",
	}
	if err := survey.AskOne(prompt, &output); err != nil {
		return output, err
	}
	return output, nil
}

// AskConfirmUninstall asks the user to confirm uninstallation
func (*ExtensionSurvey) AskConfirmUninstall(commandName string) (bool, error) {
	var output bool
	prompt := &survey.Confirm{
		Message: fmt.Sprintf("Do you want to uninstall the whole [%s] extension?", commandName),
		Help:    fmt.Sprintf("If yes, then the [%s] extension will be uninstalled", commandName),
	}
	if err := survey.AskOne(prompt, &output); err != nil {
		return output, err
	}
	return output, nil
}
