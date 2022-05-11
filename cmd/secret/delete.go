package secret

import (
	"context"
	"errors"
	"fmt"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/config"
)

type deleteCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	namespaceName string
}

// NewDeleteCommand initializes command to delete secret
func NewDeleteCommand(clientConfig *config.ClientConfig) *cobra.Command {
	dlt := &deleteCommand{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use:     "delete",
		Short:   "Delete a secrets registered with optimus",
		Example: "optimus secret delete <secret_name>",
		Long:    `This operation deletes a secret registered with optimus.`,
		RunE:    dlt.RunE,
		PreRunE: dlt.PreRunE,
	}
	cmd.Flags().StringP("project-name", "p", defaultProjectName, "Project name of optimus managed repository")
	cmd.Flags().StringVarP(&dlt.namespaceName, "namespace", "n", dlt.namespaceName, "Namespace name of optimus managed repository")
	return cmd
}

func (d *deleteCommand) PreRunE(_ *cobra.Command, _ []string) error {
	d.logger = logger.NewClientLogger(d.clientConfig.Log)
	return nil
}

func (d *deleteCommand) RunE(_ *cobra.Command, args []string) error {
	secretName, err := getSecretName(args)
	if err != nil {
		return err
	}

	deleteSecretRequest := &pb.DeleteSecretRequest{
		ProjectName:   d.clientConfig.Project.Name,
		SecretName:    secretName,
		NamespaceName: d.namespaceName,
	}
	return d.deleteSecret(deleteSecretRequest)
}

func (d *deleteCommand) deleteSecret(req *pb.DeleteSecretRequest) error {
	conn, err := connectivity.NewConnectivity(d.clientConfig.Host, secretTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	secret := pb.NewSecretServiceClient(conn.GetConnection())

	_, err = secret.DeleteSecret(conn.GetContext(), req)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			d.logger.Error(logger.ColoredError("Secret delete took too long, timing out"))
		}
		return fmt.Errorf("%w: request failed for deleting secret %s", err, req.SecretName)
	}
	d.logger.Info(logger.ColoredSuccess("Secret deleted"))
	return nil
}
