package secret

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const (
	secretTimeout = time.Minute * 2

	// TODO: get rid of system defined secrets
	systemDefinedSecretPrefix = "_OPTIMUS_"
)

// NewSecretCommand initializes command for secret
func NewSecretCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "secret",
		Short: "Manage secrets to be used in jobs",
	}

	cmd.AddCommand(
		NewDeleteCommand(),
		NewListCommand(),
		NewSetCommand(),
	)
	return cmd
}

func getSecretName(args []string) (string, error) {
	if len(args) < 1 {
		return "", errors.New("secret name is required")
	}
	if strings.HasPrefix(args[0], systemDefinedSecretPrefix) {
		return "", fmt.Errorf("secret name cannot be started with %s", systemDefinedSecretPrefix)
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
		secretValueBytes, err := os.ReadFile(filePath)
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
