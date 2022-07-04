package secret

import (
	"context"
	"errors"
	"fmt"

	saltConfig "github.com/odpf/salt/config"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/config"
)

type deleteCommand struct {
	logger         log.Logger
	configFilePath string
	clientConfig   *config.ClientConfig

	projectName   string
	host          string
	namespaceName string
}

// NewDeleteCommand initializes command to delete secret
func NewDeleteCommand() *cobra.Command {
	dlt := &deleteCommand{
		clientConfig: &config.ClientConfig{},
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
	if err := d.loadConfig(); err != nil {
		return err
	}

	if d.clientConfig == nil {
		d.logger = logger.NewDefaultLogger()
		cmd.MarkFlagRequired("project-name")
		cmd.MarkFlagRequired("host")
		return nil
	}

	d.logger = logger.NewClientLogger(d.clientConfig.Log)
	if d.projectName == "" {
		d.projectName = d.clientConfig.Project.Name
	}
	if d.host == "" {
		d.host = d.clientConfig.Host
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
			d.logger.Error(logger.ColoredError("Secret delete took too long, timing out"))
		}
		return fmt.Errorf("%w: request failed for deleting secret %s", err, req.SecretName)
	}
	d.logger.Info(logger.ColoredSuccess("Secret deleted"))
	return nil
}

func (d *deleteCommand) loadConfig() error {
	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(d.configFilePath)
	if err != nil {
		if errors.As(err, &saltConfig.ConfigFileNotFoundError{}) {
			d.clientConfig = nil
			return nil
		}
		return err
	}
	*d.clientConfig = *c
	return nil
}
