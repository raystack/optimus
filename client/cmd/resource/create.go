package resource

import (
	"fmt"
	"path/filepath"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/cmd/internal/survey"
	"github.com/odpf/optimus/client/local/model"
	"github.com/odpf/optimus/client/local/specio"
	"github.com/odpf/optimus/config"
)

type createCommand struct {
	logger         log.Logger
	configFilePath string

	namespaceSurvey *survey.NamespaceSurvey
}

// NewCreateCommand initializes resource create command
func NewCreateCommand() *cobra.Command {
	l := logger.NewClientLogger()
	create := &createCommand{
		logger:          l,
		namespaceSurvey: survey.NewNamespaceSurvey(l),
	}

	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a new resource",
		Example: "optimus resource create",
		RunE:    create.RunE,
	}
	cmd.Flags().StringVarP(&create.configFilePath, "config", "c", create.configFilePath, "File path for client configuration")
	cmd.MarkFlagRequired("config")
	return cmd
}

func (c createCommand) RunE(_ *cobra.Command, _ []string) error {
	cfg, err := config.LoadClientConfig(c.configFilePath)
	if err != nil {
		return err
	}

	selectedNamespace, err := c.namespaceSurvey.AskToSelectNamespace(cfg)
	if err != nil {
		return err
	}
	// TODO: re-check if datastore needs to be in slice, currently assuming
	if len(selectedNamespace.Datastore) == 0 {
		return fmt.Errorf("data store for namespace [%s] is not configured", selectedNamespace.Name)
	}

	specFS := afero.NewOsFs()
	resourceSpecReadWriter, err := specio.NewResourceSpecReadWriter(specFS)
	if err != nil {
		return err
	}
	resourceSpecCreateSurvey := survey.NewResourceSpecCreateSurvey(resourceSpecReadWriter)

	// we are using the first datastore since we want to support only one datastore for a single namespace
	rootDirPath := selectedNamespace.Datastore[0].Path
	resourceName, err := resourceSpecCreateSurvey.AskResourceSpecName(rootDirPath)
	if err != nil {
		return err
	}
	resourceType, err := resourceSpecCreateSurvey.AskResourceSpecType()
	if err != nil {
		return err
	}
	workingDirectory, err := survey.AskWorkingDirectory(specFS, rootDirPath)
	if err != nil {
		return err
	}
	resourceSpecDirectoryName, err := survey.AskDirectoryName(workingDirectory)
	if err != nil {
		return err
	}

	resourceDirectory := filepath.Join(workingDirectory, resourceSpecDirectoryName)
	if err := resourceSpecReadWriter.Write(resourceDirectory, &model.ResourceSpec{
		Version: 1,
		Name:    resourceName,
		Type:    resourceType,
	}); err != nil {
		return err
	}

	c.logger.Info("Resource spec [%s] is created successfully", resourceName)
	return nil
}

// CreateDataStoreSpecFs creates specFS for data store
func CreateDataStoreSpecFs(namespace *config.Namespace) map[string]afero.Fs {
	dtSpec := make(map[string]afero.Fs)
	for _, dsConfig := range namespace.Datastore {
		dtSpec[dsConfig.Type] = afero.NewBasePathFs(afero.NewOsFs(), dsConfig.Path)
	}
	return dtSpec
}
