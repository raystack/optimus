package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/client/cmd/internal"
	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/cmd/internal/progressbar"
	"github.com/odpf/optimus/client/cmd/internal/survey"
	nameSpcCmd "github.com/odpf/optimus/client/cmd/namespace"
	"github.com/odpf/optimus/config"
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

	onlyDryRun       bool
	ignoreDownstream bool
	allDownstream    bool
	skipConfirm      bool
	resourceName     string
	description      string
	storerName       string
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
		Example: "optimus backup create --resource <sample_resource_name>",
		RunE:    create.RunE,
		PreRunE: create.PreRunE,
	}

	create.injectFlags(cmd)

	return cmd
}

func (c *createCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.PersistentFlags().StringVarP(&c.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	cmd.Flags().StringVarP(&c.resourceName, "resource", "r", c.resourceName, "Resource name created inside the datastore")
	cmd.Flags().StringVarP(&c.description, "description", "i", c.description, "Describe intention to help identify the backup")
	cmd.Flags().StringVarP(&c.storerName, "datastore", "s", c.storerName, "Datastore type where the resource belongs")
	cmd.Flags().StringVar(&c.dsBackupConfig, "backup-config", "", "Backup config for the selected datastore (JSON format)")

	cmd.Flags().BoolVarP(&c.onlyDryRun, "dry-run", "d", c.onlyDryRun, "Only do a trial run with no permanent changes")
	cmd.Flags().BoolVar(&c.skipConfirm, "confirm", c.skipConfirm, "Skip asking for confirmation")
	cmd.Flags().BoolVarP(&c.allDownstream, "all-downstream", "", c.allDownstream, "Run backup for all downstreams across namespaces")
	cmd.Flags().BoolVar(&c.ignoreDownstream, "ignore-downstream", c.ignoreDownstream, "Do not take backups for dependent downstream resources")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&c.projectName, "project-name", "p", "", "project name of optimus managed repository")
	cmd.Flags().StringVar(&c.host, "host", "", "Optimus service endpoint url")
	cmd.Flags().StringVar(&c.namespace, "namespace", "", "Namespace name within project to be backed up")
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

	// use flag or ask namespace name
	if c.namespace == "" {
		namespace, err := c.namespaceSurvey.AskToSelectNamespace(conf)
		if err != nil {
			return err
		}
		c.namespace = namespace.Name
	}

	// use flag or ask datastore name
	if err := prepareDatastoreName(c.storerName); err != nil {
		return err
	}

	// use flag or fetched from config
	if c.dsBackupConfig != "" {
		err := json.Unmarshal([]byte(c.dsBackupConfig), &c.dsBackupConfigUnmarshaled)
		if err != nil {
			return err
		}
	} else {
		namespace, err := conf.GetNamespaceByName(c.namespace)
		if err != nil {
			return err
		}

		for _, ds := range namespace.Datastore {
			if ds.Type == c.storerName {
				c.dsBackupConfigUnmarshaled = ds.Backup
			}
		}
	}

	return nil
}

func (c *createCommand) RunE(_ *cobra.Command, _ []string) error {
	if err := c.prepareInput(); err != nil {
		return err
	}

	if err := c.runBackupDryRunRequest(); err != nil {
		c.logger.Warn("Failed to run backup dry run")
		return err
	}
	if c.onlyDryRun {
		return nil
	}

	if !c.skipConfirm {
		proceedWithBackup, err := c.backupCreateSurvey.AskConfirmToContinue()
		if err != nil {
			return err
		}
		if !proceedWithBackup {
			return nil
		}
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
		ProjectName:                 c.projectName,
		NamespaceName:               c.namespace,
		ResourceName:                c.resourceName,
		DatastoreName:               c.storerName,
		Description:                 c.description,
		Config:                      c.dsBackupConfigUnmarshaled,
		AllowedDownstreamNamespaces: nameSpcCmd.GetAllowedDownstreamNamespaces(c.namespace, c.allDownstream),
	}
	backupResponse, err := backup.CreateBackup(conn.GetContext(), backupRequest)
	spinner.Stop()

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			c.logger.Error("Backup took too long, timing out")
		}
		return fmt.Errorf("request failed to backup job %s: %w", backupRequest.ResourceName, err)
	}
	c.printBackupResponse(backupResponse)
	return nil
}

func (c *createCommand) printBackupResponse(backupResponse *pb.CreateBackupResponse) {
	c.logger.Info("Resource backup completed successfully:")
	for counter, result := range backupResponse.Urn {
		c.logger.Info("%d. %s", counter+1, result)
	}
}

func (c *createCommand) runBackupDryRunRequest() error {
	conn, err := connectivity.NewConnectivity(c.host, backupTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	request := &pb.BackupDryRunRequest{
		ProjectName:                 c.projectName,
		NamespaceName:               c.namespace,
		ResourceName:                c.resourceName,
		DatastoreName:               c.storerName,
		Description:                 c.description,
		AllowedDownstreamNamespaces: nameSpcCmd.GetAllowedDownstreamNamespaces(c.namespace, c.allDownstream),
	}
	backup := pb.NewBackupServiceClient(conn.GetConnection())
	backupDryRunResponse, err := backup.BackupDryRun(conn.GetContext(), request)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			c.logger.Error("Backup dry run took too long, timing out")
		}
		return fmt.Errorf("request failed to backup %s: %w", request.ResourceName, err)
	}

	c.printBackupDryRunResponse(request, backupDryRunResponse)
	return nil
}

func (c *createCommand) printBackupDryRunResponse(request *pb.BackupDryRunRequest, response *pb.BackupDryRunResponse) {
	if c.ignoreDownstream {
		c.logger.Warn("\nBackup list for %s. Downstreams will be ignored.", request.ResourceName)
	} else {
		c.logger.Info("\nBackup list for %s. Supported downstreams will be included.", request.ResourceName)
	}
	for counter, resource := range response.ResourceName {
		c.logger.Info("%d. %s", counter+1, resource)
	}

	if len(response.IgnoredResources) > 0 {
		c.logger.Warn("\nThese resources will be ignored:")
	}
	for counter, ignoredResource := range response.IgnoredResources {
		c.logger.Info("%d. %s", counter+1, ignoredResource)
	}
	c.logger.Info("")
}

func (c *createCommand) prepareInput() error {
	if !c.isConfigExist {
		if err := prepareDatastoreName(c.storerName); err != nil {
			return err
		}
		if c.dsBackupConfig != "" {
			err := json.Unmarshal([]byte(c.dsBackupConfig), &c.dsBackupConfigUnmarshaled)
			if err != nil {
				return err
			}
		}
	}

	if err := c.prepareResourceName(); err != nil {
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

func (c *createCommand) prepareResourceName() error {
	if c.resourceName == "" {
		resourceName, err := c.backupCreateSurvey.AskResourceName()
		if err != nil {
			return err
		}
		c.resourceName = resourceName
	}
	return nil
}

func prepareDatastoreName(datastoreName string) error {
	availableStorers := getAvailableDatastorers()
	if datastoreName == "" {
		storerName, err := survey.AskToSelectDatastorer(availableStorers)
		if err != nil {
			return err
		}
		datastoreName = storerName
	}
	datastoreName = strings.ToLower(datastoreName)
	validStore := false
	for _, s := range availableStorers {
		if s == datastoreName {
			validStore = true
		}
	}
	if !validStore {
		return fmt.Errorf("invalid datastore type, available values are: %v", availableStorers)
	}
	return nil
}
