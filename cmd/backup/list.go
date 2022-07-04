package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	saltConfig "github.com/odpf/salt/config"
	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/cmd/survey"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

type listCommand struct {
	logger         log.Logger
	configFilePath string
	clientConfig   *config.ClientConfig

	projectName string
	host        string
}

// NewListCommand initialize command to list backup
func NewListCommand() *cobra.Command {
	list := &listCommand{
		clientConfig: &config.ClientConfig{},
	}

	cmd := &cobra.Command{
		Use:     "list",
		Short:   "Get list of backups per project and datastore",
		Example: "optimus backup list",
		RunE:    list.RunE,
		PreRunE: list.PreRunE,
	}

	list.injectFlags(cmd)

	return cmd
}

func (l *listCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.PersistentFlags().StringVarP(&l.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&l.projectName, "project-name", "p", "", "project name of optimus managed repository")
	cmd.Flags().StringVar(&l.host, "host", "", "Optimus service endpoint url")
}

func (l *listCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	if err := l.loadConfig(); err != nil {
		return err
	}

	if l.clientConfig == nil {
		l.logger = logger.NewDefaultLogger()
		cmd.MarkFlagRequired("project-name")
		cmd.MarkFlagRequired("host")
		return nil
	}

	l.logger = logger.NewClientLogger(l.clientConfig.Log)
	if l.projectName == "" {
		l.projectName = l.clientConfig.Project.Name
	}
	if l.host == "" {
		l.host = l.clientConfig.Host
	}

	return nil
}

func (l *listCommand) RunE(_ *cobra.Command, _ []string) error {
	availableStorer := getAvailableDatastorers()
	storerName, err := survey.AskToSelectDatastorer(availableStorer)
	if err != nil {
		return err
	}

	listBackupsRequest := &pb.ListBackupsRequest{
		ProjectName:   l.projectName,
		DatastoreName: storerName,
	}

	conn, err := connectivity.NewConnectivity(l.host, backupTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	backup := pb.NewBackupServiceClient(conn.GetConnection())
	listBackupsResponse, err := backup.ListBackups(conn.GetContext(), listBackupsRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.logger.Error(logger.ColoredError("Getting list of backups took too long, timing out"))
			return err
		}
		return fmt.Errorf("request failed to get list of backups: %w", err)
	}

	if len(listBackupsResponse.Backups) == 0 {
		l.logger.Info(logger.ColoredNotice("No backups were found in %s project.", l.projectName))
	} else {
		l.logger.Info(logger.ColoredNotice("Recent backups"))
		result := l.stringifyBackupListResponse(listBackupsResponse)
		l.logger.Info(result)
	}
	return nil
}

func (*listCommand) stringifyBackupListResponse(listBackupsResponse *pb.ListBackupsResponse) string {
	buff := &bytes.Buffer{}
	table := tablewriter.NewWriter(buff)
	table.SetBorder(false)
	table.SetHeader([]string{
		"ID",
		"Resource",
		"Created at",
		"Ignore Downstream?",
		"TTL",
		"Description",
	})

	for _, backupSpec := range listBackupsResponse.Backups {
		ignoreDownstream := backupSpec.Config[models.ConfigIgnoreDownstream]
		ttl := backupSpec.Config[models.ConfigTTL]
		table.Append([]string{
			backupSpec.Id,
			backupSpec.ResourceName,
			backupSpec.CreatedAt.AsTime().Format(time.RFC3339),
			ignoreDownstream,
			ttl,
			backupSpec.Description,
		})
	}
	table.Render()
	return buff.String()
}

func (l *listCommand) loadConfig() error {
	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(l.configFilePath)
	if err != nil {
		if errors.As(err, &saltConfig.ConfigFileNotFoundError{}) {
			l.clientConfig = nil
			return nil
		}
		return err
	}
	*l.clientConfig = *c
	return nil
}
