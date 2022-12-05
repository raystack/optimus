package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/client/cmd/internal"
	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/cmd/internal/progressbar"
	"github.com/odpf/optimus/client/cmd/internal/survey"
	"github.com/odpf/optimus/config"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

type createCommand struct {
	logger         log.Logger
	configFilePath string
	isConfigExist  bool

	namespaceSurvey    *survey.NamespaceSurvey
	backupCreateSurvey *survey.BackupCreateSurvey

	projectName               string
	host                      string
	namespace                 string
	dsBackupConfig            string
	dsBackupConfigUnmarshaled map[string]string // unmarshaled version of datastoreConfig

	resourceNames []string
	description   string
	storeName     string
}

// NewCreateCommand initializes command to create backup
func NewCreateCommand() *cobra.Command {
	l := logger.NewClientLogger()
	create := &createCommand{
		logger:             l,
		namespaceSurvey:    survey.NewNamespaceSurvey(l),
		backupCreateSurvey: survey.NewBackupCreateSurvey(l),
	}

	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a backup",
		Example: "optimus backup create --resources <sample_resource_name>",
		RunE:    create.RunE,
		PreRunE: create.PreRunE,
	}

	create.injectFlags(cmd)

	return cmd
}

func (c *createCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.PersistentFlags().StringVarP(&c.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	cmd.Flags().StringSliceVarP(&c.resourceNames, "resources", "r", c.resourceNames, "Resource names created inside the datastore")
	cmd.Flags().StringVarP(&c.description, "description", "i", c.description, "Describe intention to help identify the backup")
	cmd.Flags().StringVarP(&c.storeName, "datastore", "s", "bigquery", "Datastore type where the resource belongs")
	cmd.Flags().StringVar(&c.dsBackupConfig, "backup-config", "", "Backup config for the selected datastore (JSON format)")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&c.projectName, "project-name", "p", "", "project name of optimus managed repository")
	cmd.Flags().StringVar(&c.host, "host", "", "Optimus service endpoint url")
	cmd.Flags().StringVarP(&c.namespace, "namespace", "n", "", "Namespace name within project to be backed up")
}

func (c *createCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(c.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		c.isConfigExist = false
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host", "namespace"})
		return nil
	}

	c.isConfigExist = true
	return c.fillAttributes(conf)
}

func (c *createCommand) fillAttributes(conf *config.ClientConfig) error {
	if c.projectName == "" {
		c.projectName = conf.Project.Name
	}
	if c.host == "" {
		c.host = conf.Host
	}

	var namespace *config.Namespace
	// use flag or ask namespace name
	if c.namespace == "" {
		var err error
		namespace, err = c.namespaceSurvey.AskToSelectNamespace(conf)
		if err != nil {
			return err
		}
		c.namespace = namespace.Name
	}

	if !isStoreNameValid(c.storeName, namespace) {
		return errors.New("name of datastore is invalid " + c.storeName)
	}

	// use flag or fetched from config
	if c.dsBackupConfig != "" {
		err := json.Unmarshal([]byte(c.dsBackupConfig), &c.dsBackupConfigUnmarshaled)
		if err != nil {
			return err
		}
	} else {
		for _, ds := range namespace.Datastore {
			if ds.Type == c.storeName {
				c.dsBackupConfigUnmarshaled = ds.Backup
			}
		}
	}

	return nil
}

func isStoreNameValid(name string, namespace *config.Namespace) bool {
	for _, ds := range namespace.Datastore {
		if ds.Type == name {
			return true
		}
	}
	return false
}

func (c *createCommand) RunE(_ *cobra.Command, _ []string) error {
	if err := c.prepareInput(); err != nil {
		return err
	}

	return c.runBackupRequest()
}

func (c *createCommand) runBackupRequest() error {
	conn, err := connectivity.NewConnectivity(c.host, backupTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	backup := pb.NewBackupServiceClient(conn.GetConnection())

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")

	backupRequest := &pb.CreateBackupRequest{
		ProjectName:   c.projectName,
		NamespaceName: c.namespace,
		ResourceNames: c.resourceNames,
		DatastoreName: c.storeName,
		Description:   c.description,
		Config:        c.dsBackupConfigUnmarshaled,
	}
	backupResponse, err := backup.CreateBackup(conn.GetContext(), backupRequest)
	spinner.Stop()

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			c.logger.Error("Backup took too long, timing out")
		}
		return fmt.Errorf("request failed to backup resourcse: %w", err)
	}
	c.printBackupResponse(backupResponse)
	return nil
}

func (c *createCommand) printBackupResponse(backupResponse *pb.CreateBackupResponse) {
	c.logger.Info("Resource backup completed successfully: %s", backupResponse.BackupId)
	for counter, result := range backupResponse.ResourceNames {
		c.logger.Info("%d. %s", counter+1, result)
	}
	if len(backupResponse.IgnoredResources) > 0 {
		c.logger.Info("Some resources were ignored during backing")
		for counter, result := range backupResponse.IgnoredResources {
			c.logger.Info("%d. %s : %s", counter+1, result.Name, result.Reason)
		}
	}
}

func (c *createCommand) prepareInput() error {
	if !c.isConfigExist {
		if c.dsBackupConfig != "" {
			err := json.Unmarshal([]byte(c.dsBackupConfig), &c.dsBackupConfigUnmarshaled)
			if err != nil {
				return err
			}
		}
	}

	if err := c.prepareResourceNames(); err != nil {
		return err
	}
	return c.prepareDescription()
}

func (c *createCommand) prepareDescription() error {
	if c.description == "" {
		description, err := c.backupCreateSurvey.AskBackupDescription()
		if err != nil {
			return err
		}
		c.description = description
	}
	return nil
}

func (c *createCommand) prepareResourceNames() error {
	if len(c.resourceNames) == 0 {
		resourceName, err := c.backupCreateSurvey.AskResourceNames()
		if err != nil {
			return err
		}
		names := strings.Split(resourceName, ",")
		var nonEmptyNames []string
		for _, name := range names {
			if name != "" {
				trimmedName := strings.TrimSpace(name)
				nonEmptyNames = append(nonEmptyNames, trimmedName)
			}
		}
		c.resourceNames = nonEmptyNames
	}
	return nil
}
