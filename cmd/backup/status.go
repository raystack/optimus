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

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/internal"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/cmd/survey"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

type statusCommand struct {
	logger         log.Logger
	configFilePath string

	projectName string
	host        string
}

// NewStatusCommand initialize command for backup status
func NewStatusCommand() *cobra.Command {
	status := &statusCommand{}
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
}

func (s *statusCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(s.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		s.logger = logger.NewDefaultLogger()
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	s.logger = logger.NewClientLogger(conf.Log)
	if s.projectName == "" {
		s.projectName = conf.Project.Name
	}
	if s.host == "" {
		s.host = conf.Host
	}

	return nil
}

func (s *statusCommand) RunE(_ *cobra.Command, args []string) error {
	availableStorer := getAvailableDatastorers()
	storerName, err := survey.AskToSelectDatastorer(availableStorer)
	if err != nil {
		return err
	}

	getBackupRequest := &pb.GetBackupRequest{
		ProjectName:   s.projectName,
		DatastoreName: storerName,
		Id:            args[0],
	}

	conn, err := connectivity.NewConnectivity(s.host, backupTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	backup := pb.NewBackupServiceClient(conn.GetConnection())

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	backupDetailResponse, err := backup.GetBackup(conn.GetContext(), getBackupRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.logger.Error(logger.ColoredError("Getting backup detail took too long, timing out"))
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

	ttl := backupDetailResponse.Spec.Config[models.ConfigTTL]
	expiry := backupDetailResponse.Spec.CreatedAt.AsTime()
	if ttl != "" {
		ttlDuration, err := time.ParseDuration(ttl)
		if err != nil {
			s.logger.Error(logger.ColoredError("Unable to parse backup TTL: %v", err))
		} else {
			expiry = expiry.Add(ttlDuration)
		}
	}

	table.Append([]string{"ID", backupDetailResponse.Spec.Id})
	table.Append([]string{"Resource", backupDetailResponse.Spec.ResourceName})
	table.Append([]string{"Created at", backupDetailResponse.Spec.CreatedAt.AsTime().Format(time.RFC3339)})
	table.Append([]string{"Ignore downstream?", backupDetailResponse.Spec.Config[models.ConfigIgnoreDownstream]})
	table.Append([]string{"Expire at", expiry.Format(time.RFC3339)})
	table.Append([]string{"Description", backupDetailResponse.Spec.Description})
	table.Append([]string{"Result", strings.Join(backupDetailResponse.Urn, "\n")})
	table.Render()

	return buff.String()
}
