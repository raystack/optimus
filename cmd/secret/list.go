package secret

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	"github.com/odpf/salt/log"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/config"
)

type listCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig
}

// NewListCommand initializes command for listing secret
func NewListCommand(clientConfig *config.ClientConfig) *cobra.Command {
	list := &listCommand{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use:     "list",
		Short:   "Show all the secrets registered with optimus",
		Example: "optimus secret list",
		Long:    `This operation shows the secrets for project.`,
		RunE:    list.RunE,
		PreRunE: list.PreRunE,
	}
	cmd.Flags().StringP("project-name", "p", defaultProjectName, "Project name of optimus managed repository")
	return cmd
}

func (l *listCommand) PreRunE(_ *cobra.Command, _ []string) error {
	l.logger = logger.NewClientLogger(l.clientConfig.Log)
	return nil
}

func (l *listCommand) RunE(_ *cobra.Command, _ []string) error {
	updateSecretRequest := &pb.ListSecretsRequest{
		ProjectName: l.clientConfig.Project.Name,
	}
	return l.listSecret(updateSecretRequest)
}

func (l *listCommand) listSecret(req *pb.ListSecretsRequest) error {
	conn, err := connectivity.NewConnectivity(l.clientConfig.Host, secretTimeout)
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
			l.logger.Error(logger.ColoredError("Secret listing took too long, timing out"))
		}
		return fmt.Errorf("%w: request failed for listing secrets", err)
	}

	if len(listSecretsResponse.Secrets) == 0 {
		l.logger.Info(logger.ColoredNotice("No secrets were found in %s project.", req.ProjectName))
	} else {
		result := l.stringifyListOfSecrets(listSecretsResponse)
		l.logger.Info(logger.ColoredNotice("Secrets for project: %s", l.clientConfig.Project.Name))
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
