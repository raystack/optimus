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
	"github.com/odpf/optimus/models"
)

type createCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	namespaceSurvey      *survey.NamespaceSurvey
	resourceCreateSurvey *survey.ResourceCreateSurvey
}

// NewCreateCommand initializes resource create command
func NewCreateCommand(clientConfig *config.ClientConfig) *cobra.Command {
	l := logger.NewClientLogger()
	create := &createCommand{
		clientConfig:         clientConfig,
		logger:               l,
		namespaceSurvey:      survey.NewNamespaceSurvey(l),
		resourceCreateSurvey: survey.NewResourceCreateSurvey(),
	}

	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a new resource",
		Example: "optimus resource create",
		RunE:    create.RunE,
	}
	return cmd
}

func (c *createCommand) RunE(_ *cobra.Command, _ []string) error {
	namespace, err := c.namespaceSurvey.AskToSelectNamespace(c.clientConfig)
	if err != nil {
		return err
	}
	storerName, err := c.selectDatastorerName()
	if err != nil {
		return err
	}
	repoFS, ok := CreateDataStoreSpecFs(namespace)[storerName]
	if !ok {
		return fmt.Errorf("unregistered datastore, please use configuration file to set datastore path")
	}

	// find requested datastorer
	datastorer, _ := models.DatastoreRegistry.GetByName(storerName)
	// find resource type
	resourceType, err := c.selectDataStoreType(datastorer)
	if err != nil {
		return err
	}

	resourceSpecRepo := local.NewResourceSpecRepository(repoFS, datastorer)
	typeController := datastorer.Types()[models.ResourceType(resourceType)]

	// find directory to store spec
	rwd, err := survey.AskWorkingDirectory(repoFS, "")
	if err != nil {
		return err
	}
	newDirName, err := survey.AskDirectoryName(rwd)
	if err != nil {
		return err
	}

	resourceDirectory := filepath.Join(rwd, newDirName)
	resourceName, err := c.resourceCreateSurvey.AskResourceName(resourceSpecRepo, typeController, resourceDirectory)
	if err != nil {
		return err
	}

	if err := resourceSpecRepo.SaveAt(models.ResourceSpec{
		Version:   1,
		Name:      resourceName,
		Type:      models.ResourceType(resourceType),
		Datastore: datastorer,
		Assets:    typeController.DefaultAssets(),
	}, resourceDirectory); err != nil {
		return err
	}

	c.logger.Info("Resource created successfully %s", resourceName)
	return nil
}

func (*createCommand) selectDatastorerName() (string, error) {
	datastorers := []string{}
	dsRepo := models.DatastoreRegistry
	for _, s := range dsRepo.GetAll() {
		datastorers = append(datastorers, s.Name())
	}
	return survey.AskToSelectDatastorer(datastorers)
}

func (c *createCommand) selectDataStoreType(datastorer models.Datastorer) (string, error) {
	availableTypes := []string{}
	for dsType := range datastorer.Types() {
		availableTypes = append(availableTypes, dsType.String())
	}
	return c.resourceCreateSurvey.AskToSelectResourceType(availableTypes)
}

// CreateDataStoreSpecFs creates specFS for data store
func CreateDataStoreSpecFs(namespace *config.Namespace) map[string]afero.Fs {
	dtSpec := make(map[string]afero.Fs)
	for _, dsConfig := range namespace.Datastore {
		dtSpec[dsConfig.Type] = afero.NewBasePathFs(afero.NewOsFs(), dsConfig.Path)
	}
	return dtSpec
}
