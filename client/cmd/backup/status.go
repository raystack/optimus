package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/raystack/salt/log"
	"github.com/spf13/cobra"

	"github.com/raystack/optimus/client/cmd/internal"
	"github.com/raystack/optimus/client/cmd/internal/connection"
	"github.com/raystack/optimus/client/cmd/internal/logger"
	"github.com/raystack/optimus/client/cmd/internal/progressbar"
	"github.com/raystack/optimus/config"
	pb "github.com/raystack/optimus/protos/raystack/optimus/core/v1beta1"
)

type statusCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string

	projectName string
	host        string
	storeName   string
}

// NewStatusCommand initialize command for backup status
func NewStatusCommand() *cobra.Command {
	status := &statusCommand{
		logger: logger.NewClientLogger(),
	}
	cmd := &cobra.Command{
		Use:     "status",
		Short:   "Get backup info using uuid and datastore",
		Example: "optimus backup status <uuid>",
		Args:    cobra.MinimumNArgs(1),
		RunE:    status.RunE,
		PreRunE: status.PreRunE,
	}

	status.injectFlags(cmd)

	return cmd
}

func (s *statusCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.PersistentFlags().StringVarP(&s.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&s.projectName, "project-name", "p", "", "project name of optimus managed repository")
	cmd.Flags().StringVar(&s.host, "host", "", "Optimus service endpoint url")
	cmd.Flags().StringVar(&s.storeName, "datastore", "bigquery", "Name of datastore for backup")
}

func (s *statusCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(s.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	if s.projectName == "" {
		s.projectName = conf.Project.Name
	}
	if s.host == "" {
		s.host = conf.Host
	}

	s.connection = connection.New(s.logger, conf)
	return nil
}

func (s *statusCommand) RunE(_ *cobra.Command, args []string) error {
	getBackupRequest := &pb.GetBackupRequest{
		ProjectName:   s.projectName,
		DatastoreName: s.storeName,
		Id:            args[0],
	}

	conn, err := s.connection.Create(s.host)
	if err != nil {
		return err
	}
	defer conn.Close()

	backup := pb.NewBackupServiceClient(conn)

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")

	ctx, dialCancel := context.WithTimeout(context.Background(), backupTimeout)
	defer dialCancel()

	backupDetailResponse, err := backup.GetBackup(ctx, getBackupRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.logger.Error("Getting backup detail took too long, timing out")
		}
		return fmt.Errorf("request failed to get backup detail: %w", err)
	}

	result := s.stringifyBackupDetailResponse(backupDetailResponse)
	s.logger.Info(result)
	return nil
}

func (s *statusCommand) stringifyBackupDetailResponse(backupDetailResponse *pb.GetBackupResponse) string {
	buff := &bytes.Buffer{}
	table := tablewriter.NewWriter(buff)
	table.SetBorder(false)

	ttl := backupDetailResponse.Spec.Config[configTTL]
	expiry := backupDetailResponse.Spec.CreatedAt.AsTime()
	if ttl != "" {
		ttlDuration, err := time.ParseDuration(ttl)
		if err != nil {
			s.logger.Error("Unable to parse backup TTL: %v", err)
		} else {
			expiry = expiry.Add(ttlDuration)
		}
	}

	table.Append([]string{"ID", backupDetailResponse.Spec.Id})
	table.Append([]string{"Resources", strings.Join(backupDetailResponse.Spec.ResourceNames, " ,")})
	table.Append([]string{"Created at", backupDetailResponse.Spec.CreatedAt.AsTime().Format(time.RFC3339)})
	table.Append([]string{"Expire at", expiry.Format(time.RFC3339)})
	table.Append([]string{"Description", backupDetailResponse.Spec.Description})
	table.Render()

	return buff.String()
}
