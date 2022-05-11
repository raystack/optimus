package secret

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

const (
	secretTimeout      = time.Minute * 2
	defaultProjectName = "sample_project"
)

type secretCommand struct {
	configFilePath string
	clientConfig   *config.ClientConfig
}

// NewSecretCommand initializes command for secret
func NewSecretCommand() *cobra.Command {
	secret := &secretCommand{
		clientConfig: &config.ClientConfig{},
	}

	cmd := &cobra.Command{
		Use:               "secret",
		Short:             "Manage secrets to be used in jobs",
		PersistentPreRunE: secret.PersistentPreRunE,
	}
	cmd.PersistentFlags().StringVarP(&secret.configFilePath, "config", "c", secret.configFilePath, "File path for client configuration")

	cmd.AddCommand(NewDeleteCommand(secret.clientConfig))
	cmd.AddCommand(NewListCommand(secret.clientConfig))
	cmd.AddCommand(NewSetCommand(secret.clientConfig))
	return cmd
}

func (s *secretCommand) PersistentPreRunE(cmd *cobra.Command, _ []string) error {
	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(s.configFilePath, cmd.Flags())
	if err != nil {
		return err
	}
	*s.clientConfig = *c
	return nil
}

func getSecretName(args []string) (string, error) {
	if len(args) < 1 {
		return "", errors.New("secret name is required")
	}
	if strings.HasPrefix(args[0], models.SecretTypeSystemDefinedPrefix) {
		return "", fmt.Errorf("secret name cannot be started with %s", models.SecretTypeSystemDefinedPrefix)
	}
	return args[0], nil
}

func getSecretValue(args []string, filePath string, encoded bool) (string, error) {
	var secretValue string
	if filePath == "" {
		if len(args) < 2 { //nolint: gomnd
			return "", errors.New("secret value is required")
		}
		secretValue = args[1]
	} else {
		secretValueBytes, err := ioutil.ReadFile(filePath)
		if err != nil {
			return "", fmt.Errorf("%w: failed when reading secret file %s", err, filePath)
		}
		secretValue = string(secretValueBytes)
	}

	if !encoded {
		return base64.StdEncoding.EncodeToString([]byte(secretValue)), nil
	}
	if err := validateProperlyEncoded(secretValue); err != nil {
		return "", err
	}
	return secretValue, nil
}

func validateProperlyEncoded(secretValue string) error {
	if _, err := base64.StdEncoding.DecodeString(secretValue); err != nil {
		return errors.New("value is not encoded, please remove --base64 to let Optimus encode the secret for you")
	}
	return nil
}
