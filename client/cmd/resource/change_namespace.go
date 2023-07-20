package resource

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/raystack/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/raystack/optimus/client/cmd/internal"
	"github.com/raystack/optimus/client/cmd/internal/connection"
	"github.com/raystack/optimus/client/cmd/internal/logger"
	"github.com/raystack/optimus/client/local/specio"
	"github.com/raystack/optimus/config"
	"github.com/raystack/optimus/core/resource"
	"github.com/raystack/optimus/internal/errors"
	pb "github.com/raystack/optimus/protos/raystack/optimus/core/v1beta1"
)

const (
	changeNamespaceTimeout = time.Minute * 1
)

type changeNamespaceCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string
	clientConfig   *config.ClientConfig

	project      string
	oldNamespace string
	newNamespace string
	dataStore    string
	host         string
}

// NewChangeNamespaceCommand initializes resource namespace change command
func NewChangeNamespaceCommand() *cobra.Command {
	l := logger.NewClientLogger()
	changeNamespace := &changeNamespaceCommand{
		logger: l,
	}
	cmd := &cobra.Command{
		Use:      "change-namespace",
		Short:    "Change namespace of a resource",
		Example:  "optimus resource change-namespace <resource-name> <datastore-name> --old-namespace <old-namespace> --new-namespace <new-namespace>",
		Args:     cobra.MinimumNArgs(2), //nolint
		PreRunE:  changeNamespace.PreRunE,
		RunE:     changeNamespace.RunE,
		PostRunE: changeNamespace.PostRunE,
	}
	// Config filepath flag
	cmd.Flags().StringVarP(&changeNamespace.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	internal.MarkFlagsRequired(cmd, []string{"old-namespace", "new-namespace"})
	changeNamespace.injectFlags(cmd)

	return cmd
}

func (c *changeNamespaceCommand) injectFlags(cmd *cobra.Command) {
	// Mandatory flags
	cmd.Flags().StringVarP(&c.oldNamespace, "old-namespace", "o", "", "current namespace of the resource")
	cmd.Flags().StringVarP(&c.newNamespace, "new-namespace", "n", "", "namespace to which the resource needs to be moved to")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&c.project, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&c.host, "host", "", "Optimus service endpoint url")
}

func (c *changeNamespaceCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load mandatory config
	conf, err := config.LoadClientConfig(c.configFilePath)
	if err != nil {
		return err
	}

	c.clientConfig = conf
	c.connection = connection.New(c.logger, c.clientConfig)
	return err
}

func (c *changeNamespaceCommand) RunE(_ *cobra.Command, args []string) error {
	resourceFullName := args[0]
	c.dataStore = args[1]
	err := c.sendChangeNamespaceRequest(resourceFullName)
	if err != nil {
		return fmt.Errorf("namespace change request failed for resource %s: %w", resourceFullName, err)
	}
	c.logger.Info("successfully changed namespace and deployed new DAG on Scheduler")
	return nil
}

func (c *changeNamespaceCommand) sendChangeNamespaceRequest(resourceName string) error {
	conn, err := c.connection.Create(c.host)
	if err != nil {
		return err
	}
	defer conn.Close()

	// fetch Instance by calling the optimus API
	resourceRunServiceClient := pb.NewResourceServiceClient(conn)
	request := &pb.ChangeResourceNamespaceRequest{
		ProjectName:      c.project,
		NamespaceName:    c.oldNamespace,
		DatastoreName:    c.dataStore,
		ResourceName:     resourceName,
		NewNamespaceName: c.newNamespace,
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), changeNamespaceTimeout)
	defer cancelFunc()

	_, err = resourceRunServiceClient.ChangeResourceNamespace(ctx, request)
	return err
}

func (c *changeNamespaceCommand) PostRunE(_ *cobra.Command, args []string) error {
	c.logger.Info("\n[INFO] Moving resource in filesystem")
	resourceName := args[0]
	c.dataStore = args[1]

	oldNamespaceConfig, err := c.getResourceDatastoreConfig(c.oldNamespace, c.dataStore)
	if err != nil {
		c.logger.Error(fmt.Sprintf("[error] old namespace unregistered in filesystem, err: %s", err.Error()))
		return nil
	}

	resourceSpecReadWriter, err := specio.NewResourceSpecReadWriter(afero.NewOsFs())
	if err != nil {
		c.logger.Error(fmt.Sprintf("[error] could not instantiate Spec Readed, err: %s", err.Error()))
		return nil
	}

	resourceSpec, err := resourceSpecReadWriter.ReadByName(oldNamespaceConfig.Path, resourceName)
	if err != nil {
		c.logger.Error(fmt.Sprintf("[error] unable to find resource in old namespace directory, err: %s", err.Error()))
		return nil
	}

	fs := afero.NewOsFs()
	newNamespaceConfig, err := c.getResourceDatastoreConfig(c.newNamespace, c.dataStore)
	if err != nil || newNamespaceConfig.Path == "" {
		c.logger.Warn("[warn] new namespace not recognised for Resources")
		c.logger.Warn("[info] run `optimus resource export` on the new namespace repo, to fetch the newly moved resource.")

		c.logger.Warn("[info] removing resource from old namespace")
		err = fs.RemoveAll(resourceSpec.Path)
		if err != nil {
			c.logger.Error(fmt.Sprintf("[error] unable to remove resource from old namespace , err: %s", err.Error()))
			c.logger.Warn("[info] consider deleting source files manually if they exist")
			return nil
		}
		c.logger.Warn("[OK] removed resource spec from current namespace directory")
		return nil
	}

	newResourcePath := strings.Replace(resourceSpec.Path, oldNamespaceConfig.Path, newNamespaceConfig.Path, 1)

	c.logger.Info(fmt.Sprintf("\t* Old Path : '%s' \n\t* New Path : '%s' \n", resourceSpec.Path, newResourcePath))

	c.logger.Info(fmt.Sprintf("[info] creating Resource directry: %s", newResourcePath))

	err = fs.MkdirAll(filepath.Dir(newResourcePath), os.FileMode(0o755))
	if err != nil {
		c.logger.Error(fmt.Sprintf("[error] unable to create path in the new namespace directory, err: %s", err.Error()))
		c.logger.Warn("[warn] unable to move resource from old namespace")
		c.logger.Warn("[info] consider moving source files manually")
		return nil
	}

	err = fs.Rename(resourceSpec.Path, newResourcePath)
	if err != nil {
		c.logger.Error(fmt.Sprintf("[warn] unable to move resource from old namespace, err: %s", err.Error()))
		c.logger.Warn("[info] consider moving source files manually")
		return nil
	}
	c.logger.Info("[OK] Resource moved successfully")
	return nil
}

func (c *changeNamespaceCommand) getResourceDatastoreConfig(namespaceName, datastoreName string) (*config.Datastore, error) {
	for _, namespace := range c.clientConfig.Namespaces {
		if namespace.Name == namespaceName {
			for _, datastore := range namespace.Datastore {
				if datastore.Type == datastoreName {
					return &datastore, nil
				}
			}
		}
	}
	return nil, errors.NotFound(resource.EntityResource, "not recognised in config")
}
