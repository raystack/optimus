package secret

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/config"
	"github.com/odpf/salt/log"
)

type listCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig
}

// NewListCommand initializes command for listing secret
func NewListCommand(logger log.Logger, clientConfig *config.ClientConfig) *cobra.Command {
	list := &listCommand{
		logger:       logger,
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use:     "list",
		Short:   "Show all the secrets registered with optimus",
		Example: "optimus secret list",
		Long:    `This operation shows the secrets for project.`,
		RunE:    list.RunE,
	}
	cmd.Flags().StringP("project-name", "p", defaultProjectName, "Project name of optimus managed repository")
	return cmd
}

func (l *listCommand) RunE(cmd *cobra.Command, args []string) error {
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
			l.logger.Error("Secret listing took too long, timing out")
		}
		return fmt.Errorf("%w: request failed for listing secrets", err)
	}

	if len(listSecretsResponse.Secrets) == 0 {
		l.logger.Info("No secrets were found in %s project.", req.ProjectName)
	} else {
		result := l.stringifyListOfSecrets(listSecretsResponse)
		l.logger.Info("Secrets for project: %s", l.clientConfig.Project.Name)
		l.logger.Info(result)
	}
	return nil
}

func (l *listCommand) stringifyListOfSecrets(listSecretsResponse *pb.ListSecretsResponse) string {
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
