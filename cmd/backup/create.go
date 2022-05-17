package backup

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	nameSpcCmd "github.com/odpf/optimus/cmd/namespace"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/cmd/survey"
	"github.com/odpf/optimus/config"
)

type createCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	namespaceSurvey    *survey.NamespaceSurvey
	backupCreateSurvey *survey.BackupCreateSurvey

	onlyDryRun       bool
	ignoreDownstream bool
	allDownstream    bool
	skipConfirm      bool
	resourceName     string
	description      string
	storerName       string
}

// NewCreateCommand initializes command to create backup
func NewCreateCommand(clientConfig *config.ClientConfig) *cobra.Command {
	create := &createCommand{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a backup",
		Example: "optimus backup create --resource <sample_resource_name>",
		RunE:    create.RunE,
		PreRunE: create.PreRunE,
	}
	cmd.Flags().StringVarP(&create.resourceName, "resource", "r", create.resourceName, "Resource name created inside the datastore")
	cmd.Flags().StringVarP(&create.description, "description", "i", create.description, "Describe intention to help identify the backup")
	cmd.Flags().StringVarP(&create.storerName, "datastore", "s", create.storerName, "Datastore type where the resource belongs")

	cmd.Flags().BoolVarP(&create.onlyDryRun, "dry-run", "d", create.onlyDryRun, "Only do a trial run with no permanent changes")
	cmd.Flags().BoolVar(&create.skipConfirm, "confirm", create.skipConfirm, "Skip asking for confirmation")
	cmd.Flags().BoolVarP(&create.allDownstream, "all-downstream", "", create.allDownstream, "Run backup for all downstreams across namespaces")
	cmd.Flags().BoolVar(&create.ignoreDownstream, "ignore-downstream", create.ignoreDownstream, "Do not take backups for dependent downstream resources")
	return cmd
}

func (c *createCommand) PreRunE(_ *cobra.Command, _ []string) error {
	c.logger = logger.NewClientLogger(c.clientConfig.Log)
	c.namespaceSurvey = survey.NewNamespaceSurvey(c.logger)
	c.backupCreateSurvey = survey.NewBackupCreateSurvey(c.logger)
	return nil
}

func (c *createCommand) RunE(_ *cobra.Command, _ []string) error {
	var err error
	namespace, err := c.namespaceSurvey.AskToSelectNamespace(c.clientConfig)
	if err != nil {
		return err
	}
	if err := c.prepareInput(); err != nil {
		return err
	}

	if err := c.runBackupDryRunRequest(namespace.Name); err != nil {
		c.logger.Info(logger.ColoredNotice("Failed to run backup dry run"))
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
	return c.runBackupRequest(namespace)
}

func (c *createCommand) runBackupRequest(namespace *config.Namespace) error {
	conn, err := connectivity.NewConnectivity(c.clientConfig.Host, backupTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	backup := pb.NewBackupServiceClient(conn.GetConnection())

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")

	backupRequest := &pb.CreateBackupRequest{
		ProjectName:                 c.clientConfig.Project.Name,
		NamespaceName:               namespace.Name,
		ResourceName:                c.resourceName,
		DatastoreName:               c.storerName,
		Description:                 c.description,
		AllowedDownstreamNamespaces: nameSpcCmd.GetAllowedDownstreamNamespaces(namespace.Name, c.allDownstream),
	}
	for _, ds := range namespace.Datastore {
		if ds.Type == c.storerName {
			backupRequest.Config = ds.Backup
		}
	}
	backupResponse, err := backup.CreateBackup(conn.GetContext(), backupRequest)
	spinner.Stop()

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			c.logger.Error(logger.ColoredError("Backup took too long, timing out"))
		}
		return fmt.Errorf("request failed to backup job %s: %w", backupRequest.ResourceName, err)
	}
	c.printBackupResponse(backupResponse)
	return nil
}

func (c *createCommand) printBackupResponse(backupResponse *pb.CreateBackupResponse) {
	c.logger.Info(logger.ColoredSuccess("Resource backup completed successfully:"))
	for counter, result := range backupResponse.Urn {
		c.logger.Info(fmt.Sprintf("%d. %s", counter+1, result))
	}
}

func (c *createCommand) runBackupDryRunRequest(namespaceName string) error {
	conn, err := connectivity.NewConnectivity(c.clientConfig.Host, backupTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	request := &pb.BackupDryRunRequest{
		ProjectName:                 c.clientConfig.Project.Name,
		NamespaceName:               namespaceName,
		ResourceName:                c.resourceName,
		DatastoreName:               c.storerName,
		Description:                 c.description,
		AllowedDownstreamNamespaces: nameSpcCmd.GetAllowedDownstreamNamespaces(namespaceName, c.allDownstream),
	}
	backup := pb.NewBackupServiceClient(conn.GetConnection())
	backupDryRunResponse, err := backup.BackupDryRun(conn.GetContext(), request)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			c.logger.Error(logger.ColoredError("Backup dry run took too long, timing out"))
		}
		return fmt.Errorf("request failed to backup %s: %w", request.ResourceName, err)
	}

	c.printBackupDryRunResponse(request, backupDryRunResponse)
	return nil
}

func (c *createCommand) printBackupDryRunResponse(request *pb.BackupDryRunRequest, response *pb.BackupDryRunResponse) {
	if c.ignoreDownstream {
		c.logger.Info(logger.ColoredNotice("\nBackup list for %s. Downstreams will be ignored.", request.ResourceName))
	} else {
		c.logger.Info(logger.ColoredNotice("\nBackup list for %s. Supported downstreams will be included.", request.ResourceName))
	}
	for counter, resource := range response.ResourceName {
		c.logger.Info(fmt.Sprintf("%d. %s", counter+1, resource))
	}

	if len(response.IgnoredResources) > 0 {
		c.logger.Info("\nThese resources will be ignored:")
	}
	for counter, ignoredResource := range response.IgnoredResources {
		c.logger.Info(fmt.Sprintf("%d. %s", counter+1, ignoredResource))
	}
	c.logger.Info("")
}

func (c *createCommand) prepareInput() error {
	if err := c.prepareDatastoreName(); err != nil {
		return err
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

func (c *createCommand) prepareDatastoreName() error {
	availableStorers := getAvailableDatastorers()
	if c.storerName == "" {
		storerName, err := survey.AskToSelectDatastorer(availableStorers)
		if err != nil {
			return err
		}
		c.storerName = storerName
	}
	c.storerName = strings.ToLower(c.storerName)
	validStore := false
	for _, s := range availableStorers {
		if s == c.storerName {
			validStore = true
		}
	}
	if !validStore {
		return fmt.Errorf("invalid datastore type, available values are: %v", availableStorers)
	}
	return nil
}
