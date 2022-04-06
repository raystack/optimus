package cmd

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	cli "github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/local"
	"github.com/odpf/optimus/utils"
)

var validateResourceName = utils.ValidatorFactory.NewFromRegex(`^[a-zA-Z0-9][a-zA-Z0-9_\-\.]+$`,
	`invalid name (can only contain characters A-Z (in either case), 0-9, "-", "_" or "." and must start with an alphanumeric character)`)

func resourceCommand() *cli.Command {
	var (
		configFilePath string
		conf           config.ClientConfig
	)

	cmd := &cli.Command{
		Use:   "resource",
		Short: "Interact with data resource",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.PersistentFlags().StringVarP(&configFilePath, "config", "c", configFilePath, "File path for client configuration")

	cmd.PersistentPreRunE = func(cmd *cli.Command, args []string) error {
		// TODO: find a way to load the config in one place
		c, err := config.LoadClientConfig(configFilePath)
		if err != nil {
			return err
		}
		conf = *c

		return nil
	}

	cmd.AddCommand(createResourceSubCommand(&conf))
	return cmd
}

func createResourceSubCommand(conf *config.ClientConfig) *cli.Command {
	cmd := &cli.Command{
		Use:     "create",
		Short:   "Create a new resource",
		Example: "optimus resource create",
	}

	cmd.RunE = func(cmd *cli.Command, args []string) error {
		l := initClientLogger(conf.Log)
		dsRepo := models.DatastoreRegistry
		datastoreSpecFs := getDatastoreSpecFs(conf.Namespaces)

		namespace, err := askToSelectNamespace(l, conf)
		if err != nil {
			return err
		}
		availableStorer := []string{}
		for _, s := range dsRepo.GetAll() {
			availableStorer = append(availableStorer, s.Name())
		}
		var storerName string
		if err := survey.AskOne(&survey.Select{
			Message: "Select supported datastores?",
			Options: availableStorer,
		}, &storerName); err != nil {
			return err
		}
		repoFS, ok := datastoreSpecFs[namespace.Name][storerName]
		if !ok {
			return fmt.Errorf("unregistered datastore, please use configuration file to set datastore path")
		}

		// find requested datastore
		availableTypes := []string{}
		datastore, _ := dsRepo.GetByName(storerName)
		for dsType := range datastore.Types() {
			availableTypes = append(availableTypes, dsType.String())
		}
		resourceSpecRepo := local.NewResourceSpecRepository(repoFS, datastore)

		// find resource type
		var resourceType string
		if err := survey.AskOne(&survey.Select{
			Message: "Select supported resource type?",
			Options: availableTypes,
		}, &resourceType); err != nil {
			return err
		}
		typeController := datastore.Types()[models.ResourceType(resourceType)]

		// find directory to store spec
		rwd, err := getWorkingDirectory(repoFS, "")
		if err != nil {
			return err
		}
		newDirName, err := getDirectoryName(rwd)
		if err != nil {
			return err
		}

		resourceDirectory := filepath.Join(rwd, newDirName)
		resourceNameDefault := strings.ReplaceAll(strings.ReplaceAll(resourceDirectory, "/", "."), "\\", ".")

		qs := []*survey.Question{
			{
				Name: "name",
				Prompt: &survey.Input{
					Message: "What is the resource name?(should conform to selected resource type)",
					Default: resourceNameDefault,
				},
				Validate: survey.ComposeValidators(validateNoSlash, survey.MinLength(3),
					survey.MaxLength(1024), IsValidDatastoreSpec(typeController.Validator()),
					IsResourceNameUnique(resourceSpecRepo)),
			},
		}
		inputs := map[string]interface{}{}
		if err := survey.Ask(qs, &inputs); err != nil {
			return err
		}
		resourceName := inputs["name"].(string)

		if err := resourceSpecRepo.SaveAt(models.ResourceSpec{
			Version:   1,
			Name:      resourceName,
			Type:      models.ResourceType(resourceType),
			Datastore: datastore,
			Assets:    typeController.DefaultAssets(),
		}, resourceDirectory); err != nil {
			return err
		}

		l.Info(coloredSuccess("Resource created successfully %s", resourceName))
		return nil
	}

	return cmd
}

// IsResourceNameUnique return a validator that checks if the resource already exists with the same name
func IsResourceNameUnique(repository store.ResourceSpecRepository) survey.Validator {
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

// IsValidDatastoreSpec tries to adapt provided resource with datastore
func IsValidDatastoreSpec(valiFn models.DatastoreSpecValidator) survey.Validator {
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
