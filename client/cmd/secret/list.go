package secret

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/client/cmd/internal"
	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/cmd/internal/progressbar"
	"github.com/odpf/optimus/config"
)

type listCommand struct {
	logger         log.Logger
	configFilePath string

	projectName string
	host        string
}

// NewListCommand initializes command for listing secret
func NewListCommand() *cobra.Command {
	list := &listCommand{
		logger: logger.NewClientLogger(),
	}
	cmd := &cobra.Command{
		Use:     "list",
		Short:   "Show all the secrets registered with optimus",
		Example: "optimus secret list",
		Long:    `This operation shows the secrets for project.`,
		RunE:    list.RunE,
		PreRunE: list.PreRunE,
	}

	list.injectFlags(cmd)
	return cmd
}

func (l *listCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&l.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&l.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&l.host, "host", "", "Optimus service endpoint url")
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
	return nil
}

func (l *listCommand) RunE(_ *cobra.Command, _ []string) error {
	updateSecretRequest := &pb.ListSecretsRequest{
		ProjectName: l.projectName,
	}
	return l.listSecret(updateSecretRequest)
}

func (l *listCommand) listSecret(req *pb.ListSecretsRequest) error {
	conn, err := connectivity.NewConnectivity(l.host, secretTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	secret := pb.NewSecretServiceClient(conn.GetConnection())

	listSecretsResponse, err := secret.ListSecrets(conn.GetContext(), req)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.logger.Error("Secret listing took too long, timing out")
		}
		return fmt.Errorf("%w: request failed for listing secrets", err)
	}

	if len(listSecretsResponse.Secrets) == 0 {
		l.logger.Info("No secrets were found in %s project.", req.ProjectName)
	} else {
		result := l.stringifyListOfSecrets(listSecretsResponse)
		l.logger.Info("Secrets for project: %s", l.projectName)
		l.logger.Info(result)
	}
	return nil
}

func (*listCommand) stringifyListOfSecrets(listSecretsResponse *pb.ListSecretsResponse) string {
	buff := &bytes.Buffer{}
	table := tablewriter.NewWriter(buff)
	table.SetBorder(false)
	table.SetHeader([]string{
		"Name",
		"Digest",
		"Namespace",
		"Date",
	})

	table.SetAlignment(tablewriter.ALIGN_CENTER)
	for _, secret := range listSecretsResponse.Secrets {
		namespace := "*"
		if secret.Namespace != "" {
			namespace = secret.Namespace
		}
		table.Append([]string{
			secret.Name,
			secret.Digest,
			namespace,
			secret.UpdatedAt.AsTime().Format(time.RFC3339),
		})
	}
	table.Render()
	return buff.String()
}
