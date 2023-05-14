package survey

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/AlecAivazis/survey/v2"

	"github.com/odpf/optimus/client/local"
	"github.com/odpf/optimus/client/local/model"
)

// ResourceSpecCreateSurvey defines surveys for resource spec creation
type ResourceSpecCreateSurvey struct {
	resourceSpecReader local.SpecReader[*model.ResourceSpec]
}

// NewResourceSpecCreateSurvey initializes survey for resource spec create
func NewResourceSpecCreateSurvey(resourceSpecReader local.SpecReader[*model.ResourceSpec]) *ResourceSpecCreateSurvey {
	return &ResourceSpecCreateSurvey{
		resourceSpecReader: resourceSpecReader,
	}
}

// AskResourceSpecName asks the user to input the required resource spec name
func (r ResourceSpecCreateSurvey) AskResourceSpecName(rootDirPath string) (string, error) {
	defaultResourceName := strings.ReplaceAll(strings.ReplaceAll(rootDirPath, "/", "."), "\\", ".")
	qs := []*survey.Question{
		{
			Name: "name",
			Prompt: &survey.Input{
				Message: "What is the resource name?",
				Default: defaultResourceName,
			},
			Validate: survey.ComposeValidators(
				validateNoSlash,
				survey.MinLength(3),
				survey.MaxLength(1024),
				r.isResourceSpecNameUnique(rootDirPath)),
		},
	}
	inputs := map[string]interface{}{}
	if err := survey.Ask(qs, &inputs); err != nil {
		return "", err
	}
	return inputs["name"].(string), nil
}

func (ResourceSpecCreateSurvey) AskResourceSpecType() (string, error) {
	var resourceSpecType string
	if err := survey.AskOne(
		&survey.Input{
			Message: "What is the resource type?",
		},
		&resourceSpecType,
		survey.WithValidator(
			survey.ComposeValidators(survey.Required),
		),
	); err != nil {
		return "", err
	}
	return resourceSpecType, nil
}

func (r ResourceSpecCreateSurvey) isResourceSpecNameUnique(rootDirPath string) survey.Validator {
	return func(val interface{}) error {
		str, ok := val.(string)
		if !ok {
			return fmt.Errorf("invalid type of resource name %v", reflect.TypeOf(val).Name())
		}
		if _, err := r.resourceSpecReader.ReadByName(rootDirPath, str); err == nil {
			return fmt.Errorf("resource with the provided name already exists")
		} else if !strings.Contains(err.Error(), "not found") {
			return err
		}
		return nil
	}
}
