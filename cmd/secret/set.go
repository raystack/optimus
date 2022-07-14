package secret

import (
	"context"
	"errors"
	"fmt"

	saltConfig "github.com/odpf/salt/config"
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
	logger         log.Logger
	configFilePath string
	clientConfig   *config.ClientConfig

	survey *survey.SecretSetSurvey

	projectName   string
	host          string
	namespaceName string
	filePath      string
	encoded       bool
	updateOnly    bool
	skipConfirm   bool
}

// NewSetCommand initializes command for setting secret
func NewSetCommand() *cobra.Command {
	set := &setCommand{
		clientConfig: &config.ClientConfig{},
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

	set.injectFlags(cmd)

	return cmd
}

func (s *setCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&s.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	cmd.Flags().StringVarP(&s.namespaceName, "namespace", "n", s.namespaceName, "Namespace of deployee")
	cmd.Flags().BoolVar(&s.encoded, "base64", false, "Create secret with value that has been encoded")
	cmd.Flags().BoolVar(&s.updateOnly, "update-only", false, "Only update existing secret, do not create new")
	cmd.Flags().StringVarP(&s.filePath, "file", "f", s.filePath, "Provide file path to create secret from file instead")
	cmd.Flags().BoolVar(&s.skipConfirm, "confirm", false, "Skip asking for confirmation")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&s.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&s.host, "host", "", "Optimus service endpoint url")
}

func (s *setCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	if err := s.loadConfig(); err != nil {
		return err
	}

	if s.clientConfig == nil {
		s.logger = logger.NewDefaultLogger()
		s.survey = survey.NewSecretSetSurvey()
		markFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	s.logger = logger.NewClientLogger(s.clientConfig.Log)
	s.survey = survey.NewSecretSetSurvey()
	if s.projectName == "" {
		s.projectName = s.clientConfig.Project.Name
	}
	if s.host == "" {
		s.host = s.clientConfig.Host
	}

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
			ProjectName:   s.projectName,
			SecretName:    secretName,
			Value:         secretValue,
			NamespaceName: s.namespaceName,
		}
		return s.updateSecret(updateSecretRequest)
	}

	registerSecretReq := &pb.RegisterSecretRequest{
		ProjectName:   s.projectName,
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
					ProjectName:   s.projectName,
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
	conn, err := connectivity.NewConnectivity(s.host, secretTimeout)
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
	conn, err := connectivity.NewConnectivity(s.host, secretTimeout)
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

func (s *setCommand) loadConfig() error {
	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(s.configFilePath)
	if err != nil {
		if errors.As(err, &saltConfig.ConfigFileNotFoundError{}) {
			s.clientConfig = nil
			return nil
		}
		return err
	}
	*s.clientConfig = *c
	return nil
}
