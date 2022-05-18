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
