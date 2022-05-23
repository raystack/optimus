package secret

import (
	"context"
	"errors"
	"fmt"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/cmd/survey"
	"github.com/odpf/optimus/config"
)

type setCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	survey *survey.SecretSetSurvey

	namespaceName string
	filePath      string
	encoded       bool
	updateOnly    bool
	skipConfirm   bool
}

// NewSetCommand initializes command for setting secret
func NewSetCommand(clientConfig *config.ClientConfig) *cobra.Command {
	set := &setCommand{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use:     "set",
		Short:   "Register secret with optimus",
		Example: "optimus secret set <secret_name> <secret_value>",
		Long: `
This operation takes secret name as its first argument. 
Secret value can be either provided in second argument or through file flag. 
Use base64 flag if the value has been encoded.
		`,
		RunE:    set.RunE,
		PreRunE: set.PreRunE,
	}
	cmd.Flags().StringP("project-name", "p", defaultProjectName, "Project name of optimus managed repository")
	cmd.Flags().StringVarP(&set.namespaceName, "namespace", "n", set.namespaceName, "Namespace of deployee")
	cmd.Flags().BoolVar(&set.encoded, "base64", false, "Create secret with value that has been encoded")
	cmd.Flags().BoolVar(&set.updateOnly, "update-only", false, "Only update existing secret, do not create new")
	cmd.Flags().StringVarP(&set.filePath, "file", "f", set.filePath, "Provide file path to create secret from file instead")
	cmd.Flags().BoolVar(&set.skipConfirm, "confirm", false, "Skip asking for confirmation")

	return cmd
}

func (s *setCommand) PreRunE(_ *cobra.Command, _ []string) error {
	s.logger = logger.NewClientLogger(s.clientConfig.Log)
	s.survey = survey.NewSecretSetSurvey()
	return nil
}

func (s *setCommand) RunE(_ *cobra.Command, args []string) error {
	secretName, err := getSecretName(args)
	if err != nil {
		return err
	}
	secretValue, err := getSecretValue(args, s.filePath, s.encoded)
	if err != nil {
		return err
	}

	if s.updateOnly {
		updateSecretRequest := &pb.UpdateSecretRequest{
			ProjectName:   s.clientConfig.Project.Name,
			SecretName:    secretName,
			Value:         secretValue,
			NamespaceName: s.namespaceName,
		}
		return s.updateSecret(updateSecretRequest)
	}

	registerSecretReq := &pb.RegisterSecretRequest{
		ProjectName:   s.clientConfig.Project.Name,
		SecretName:    secretName,
		Value:         secretValue,
		NamespaceName: s.namespaceName,
	}
	err = s.registerSecret(registerSecretReq)
	if err != nil {
		if status.Code(err) == codes.AlreadyExists {
			proceedWithUpdate, err := s.survey.AskToConfirmUpdate()
			if err != nil {
				return err
			}
			if proceedWithUpdate {
				updateSecretRequest := &pb.UpdateSecretRequest{
					ProjectName:   s.clientConfig.Project.Name,
					SecretName:    secretName,
					Value:         secretValue,
					NamespaceName: s.namespaceName,
				}
				return s.updateSecret(updateSecretRequest)
			}
			s.logger.Info(logger.ColoredNotice("Aborting..."))
			return nil
		}
		return fmt.Errorf("%w: request failed for creating secret %s", err, secretName)
	}
	return nil
}

func (s *setCommand) registerSecret(req *pb.RegisterSecretRequest) error {
	conn, err := connectivity.NewConnectivity(s.clientConfig.Host, secretTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	secret := pb.NewSecretServiceClient(conn.GetConnection())

	_, err = secret.RegisterSecret(conn.GetContext(), req)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.logger.Error(logger.ColoredError("Secret registration took too long, timing out"))
		}
		return err
	}
	s.logger.Info(logger.ColoredSuccess("Secret registered"))
	return nil
}

func (s *setCommand) updateSecret(req *pb.UpdateSecretRequest) error {
	conn, err := connectivity.NewConnectivity(s.clientConfig.Host, secretTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	secret := pb.NewSecretServiceClient(conn.GetConnection())

	_, err = secret.UpdateSecret(conn.GetContext(), req)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.logger.Error(logger.ColoredError("Secret update took too long, timing out"))
		}
		return fmt.Errorf("%w: request failed for updating secret %s", err, req.SecretName)
	}
	s.logger.Info(logger.ColoredSuccess("Secret updated"))
	return nil
}
