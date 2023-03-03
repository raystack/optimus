package secret

import (
	"context"
	"errors"
	"fmt"

	"github.com/goto/salt/log"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connectivity"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/progressbar"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

type deleteCommand struct {
	logger         log.Logger
	configFilePath string

	projectName   string
	host          string
	namespaceName string
}

// NewDeleteCommand initializes command to delete secret
func NewDeleteCommand() *cobra.Command {
	dlt := &deleteCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "delete",
		Short:   "Delete a secrets registered with optimus",
		Example: "optimus secret delete <secret_name>",
		Long:    `This operation deletes a secret registered with optimus.`,
		RunE:    dlt.RunE,
		PreRunE: dlt.PreRunE,
	}
	dlt.injectFlags(cmd)
	return cmd
}

func (d *deleteCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&d.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	cmd.Flags().StringVarP(&d.namespaceName, "namespace", "n", d.namespaceName, "Namespace name of optimus managed repository")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&d.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&d.host, "host", "", "Optimus service endpoint url")
}

func (d *deleteCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(d.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	if d.projectName == "" {
		d.projectName = conf.Project.Name
	}
	if d.host == "" {
		d.host = conf.Host
	}
	return nil
}

func (d *deleteCommand) RunE(_ *cobra.Command, args []string) error {
	secretName, err := getSecretName(args)
	if err != nil {
		return err
	}

	deleteSecretRequest := &pb.DeleteSecretRequest{
		ProjectName:   d.projectName,
		SecretName:    secretName,
		NamespaceName: d.namespaceName,
	}
	return d.deleteSecret(deleteSecretRequest)
}

func (d *deleteCommand) deleteSecret(req *pb.DeleteSecretRequest) error {
	conn, err := connectivity.NewConnectivity(d.host, secretTimeout)
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
			d.logger.Error("Secret delete took too long, timing out")
		}
		return fmt.Errorf("%w: request failed for deleting secret %s", err, req.SecretName)
	}
	d.logger.Info("Secret deleted")
	return nil
}
