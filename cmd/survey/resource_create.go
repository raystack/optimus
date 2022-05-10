package survey

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

// ResourceCreateSurvey defines surveys for resource creation
type ResourceCreateSurvey struct{}

// NewResourceCreateSurvey initializes survey for resource create
func NewResourceCreateSurvey() *ResourceCreateSurvey {
	return nil
}

// AskToSelectResourceType asks the user to select resource type
func (r *ResourceCreateSurvey) AskToSelectResourceType(types []string) (string, error) {
	var resourceType string
	return resourceType, survey.AskOne(
		&survey.Select{
			Message: "Select supported resource type?",
			Options: types,
		},
		&resourceType,
	)
}

// AskResourceName asks the user to input the required resource name
func (r *ResourceCreateSurvey) AskResourceName(
	resourceSpecRepo store.ResourceSpecRepository,
	typeController models.DatastoreTypeController,
	resourceDirPath string,
) (string, error) {
	resourceNameDefault := strings.ReplaceAll(strings.ReplaceAll(resourceDirPath, "/", "."), "\\", ".")

	qs := []*survey.Question{
		{
			Name: "name",
			Prompt: &survey.Input{
				Message: "What is the resource name?(should conform to selected resource type)",
				Default: resourceNameDefault,
			},
			Validate: survey.ComposeValidators(validateNoSlash, survey.MinLength(3),
				survey.MaxLength(1024), r.isValidDatastoreSpec(typeController.Validator()),
				r.isResourceNameUnique(resourceSpecRepo)),
		},
	}
	inputs := map[string]interface{}{}
	if err := survey.Ask(qs, &inputs); err != nil {
		return "", err
	}
	return inputs["name"].(string), nil
}

// isResourceNameUnique return a validator that checks if the resource already exists with the same name
func (r *ResourceCreateSurvey) isResourceNameUnique(repository store.ResourceSpecRepository) survey.Validator {
	return func(val interface{}) error {
		if str, ok := val.(string); ok {
			if _, err := repository.GetByName(context.Background(), str); err == nil {
				return fmt.Errorf("resource with the provided name already exists")
			} else if !errors.Is(err, models.ErrNoSuchSpec) && !errors.Is(err, models.ErrNoResources) {
				return err
			}
		} else {
			// otherwise we cannot convert the value into a string and cannot find a resource name
			return fmt.Errorf("invalid type of resource name %v", reflect.TypeOf(val).Name())
		}
		// the input is fine
		return nil
	}
}

// isValidDatastoreSpec tries to adapt provided resource with datastore
func (r *ResourceCreateSurvey) isValidDatastoreSpec(valiFn models.DatastoreSpecValidator) survey.Validator {
	return func(val interface{}) error {
		if str, ok := val.(string); ok {
			if err := valiFn(models.ResourceSpec{
				Name: str,
			}); err != nil {
				return err
			}
		} else {
			// otherwise we cannot convert the value into a string and cannot find a resource name
			return fmt.Errorf("invalid type of resource name %v", reflect.TypeOf(val).Name())
		}
		// the input is fine
		return nil
	}
}
