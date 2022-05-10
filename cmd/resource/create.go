package resource

import (
	"fmt"
	"path/filepath"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	oSurvey "github.com/odpf/optimus/cmd/survey"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
)

type createCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	namespaceSurvey      *oSurvey.NamespaceSurvey
	resourceCreateSurvey *oSurvey.ResourceCreateSurvey
}

// NewCreateCommand initializes resource create command
func NewCreateCommand(logger log.Logger, clientConfig *config.ClientConfig) *cobra.Command {
	create := &createCommand{
		logger:               logger,
		clientConfig:         clientConfig,
		namespaceSurvey:      oSurvey.NewNamespaceSurvey(logger),
		resourceCreateSurvey: oSurvey.NewResourceCreateSurvey(),
	}

	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a new resource",
		Example: "optimus resource create",
		RunE:    create.RunE,
	}
	return cmd
}

func (c *createCommand) RunE(cmd *cobra.Command, args []string) error {
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
	rwd, err := oSurvey.AskWorkingDirectory(repoFS, "")
	if err != nil {
		return err
	}
	newDirName, err := oSurvey.AskDirectoryName(rwd)
	if err != nil {
		return err
	}

	resourceDirectory := filepath.Join(rwd, newDirName)
	resourceName, err := c.resourceCreateSurvey.AskResourceName(resourceSpecRepo, typeController, resourceDirectory)

	if err := resourceSpecRepo.SaveAt(models.ResourceSpec{
		Version:   1,
		Name:      resourceName,
		Type:      models.ResourceType(resourceType),
		Datastore: datastorer,
		Assets:    typeController.DefaultAssets(),
	}, resourceDirectory); err != nil {
		return err
	}

	c.logger.Info(fmt.Sprintf("Resource created successfully %s", resourceName))
	return nil
}

func (c *createCommand) selectDatastorerName() (string, error) {
	datastorers := []string{}
	dsRepo := models.DatastoreRegistry
	for _, s := range dsRepo.GetAll() {
		datastorers = append(datastorers, s.Name())
	}
	return c.resourceCreateSurvey.AskToSelectDatastorer(datastorers)
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
