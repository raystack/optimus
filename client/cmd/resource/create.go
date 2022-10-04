package resource

import (
	"fmt"
	"path/filepath"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/cmd/internal/survey"
	"github.com/odpf/optimus/client/local"
	"github.com/odpf/optimus/config"
)

type createCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	namespaceSurvey *survey.NamespaceSurvey
}

// NewCreateCommand initializes resource create command
func NewCreateCommand(clientConfig *config.ClientConfig) *cobra.Command {
	l := logger.NewClientLogger()
	create := &createCommand{
		clientConfig:    clientConfig,
		logger:          l,
		namespaceSurvey: survey.NewNamespaceSurvey(l),
	}

	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a new resource",
		Example: "optimus resource create",
		RunE:    create.RunE,
	}
	return cmd
}

func (c createCommand) RunE(_ *cobra.Command, _ []string) error {
	selectedNamespace, err := c.namespaceSurvey.AskToSelectNamespace(c.clientConfig)
	if err != nil {
		return err
	}
	// TODO: re-check if datastore needs to be in slice, currently assuming
	if len(selectedNamespace.Datastore) == 0 {
		return fmt.Errorf("data store for selected namespace [%s] is not configured", selectedNamespace.Name)
	}

	specFS := afero.NewOsFs()
	resourceSpecReadWriter, err := local.NewResourceSpecReadWriter(specFS)
	if err != nil {
		return err
	}
	resourceSpecCreateSurvey := survey.NewResourceSpecCreateSurvey(resourceSpecReadWriter)

	rootDirPath := selectedNamespace.Datastore[0].Path
	resourceName, err := resourceSpecCreateSurvey.AskResourceSpecName(rootDirPath)
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
	if err := resourceSpecReadWriter.Write(resourceDirectory, &local.ResourceSpec{
		Version: 1,
		Name:    resourceName,
		Type:    selectedNamespace.Datastore[0].Type,
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
