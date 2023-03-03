package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connectivity"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/progressbar"
	"github.com/goto/optimus/client/cmd/internal/survey"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	configTTL = "ttl"
)

type listCommand struct {
	logger         log.Logger
	configFilePath string

	namespaceSurvey *survey.NamespaceSurvey

	projectName   string
	namespaceName string
	host          string
	storeName     string
}

// NewListCommand initialize command to list backup
func NewListCommand() *cobra.Command {
	list := &listCommand{
		logger: logger.NewClientLogger(),
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
	cmd.Flags().StringVar(&l.storeName, "datastore", "bigquery", "Name of datastore for backup")
	cmd.Flags().StringVarP(&l.namespaceName, "namespace", "n", "", "Namespace name within project to be backed up")
}

func (l *listCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(l.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	if l.projectName == "" {
		l.projectName = conf.Project.Name
	}
	if l.host == "" {
		l.host = conf.Host
	}
	// use flag or ask namespace name
	if l.namespaceName == "" {
		namespace, err := l.namespaceSurvey.AskToSelectNamespace(conf)
		if err != nil {
			return err
		}
		l.namespaceName = namespace.Name
	}

	return nil
}

func (l *listCommand) RunE(_ *cobra.Command, _ []string) error {
	listBackupsRequest := &pb.ListBackupsRequest{
		ProjectName:   l.projectName,
		DatastoreName: l.storeName,
		NamespaceName: l.namespaceName,
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
			l.logger.Error("Getting list of backups took too long, timing out")
			return err
		}
		return fmt.Errorf("request failed to get list of backups: %w", err)
	}

	if len(listBackupsResponse.Backups) == 0 {
		l.logger.Warn("No backups were found in %s project.", l.projectName)
	} else {
		l.logger.Info("Recent backups")
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
		"TTL",
		"Description",
	})

	for _, backupSpec := range listBackupsResponse.Backups {
		ttl := backupSpec.Config[configTTL]
		table.Append([]string{
			backupSpec.Id,
			strings.Join(backupSpec.ResourceNames, ", "),
			backupSpec.CreatedAt.AsTime().Format(time.RFC3339),
			ttl,
			backupSpec.Description,
		})
	}
	table.Render()
	return buff.String()
}
