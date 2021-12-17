package cmd

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/odpf/optimus/models"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	secretTimeout = time.Minute * 2
)

func secretCommand(l log.Logger, conf config.Provider) *cli.Command {
	cmd := &cli.Command{
		Use:   "secret",
		Short: "Manage secrets to be used in jobs",
	}
	cmd.AddCommand(secretSetSubCommand(l, conf))
	return cmd
}

func secretSetSubCommand(l log.Logger, conf config.Provider) *cli.Command {
	var (
		projectName   string
		namespaceName string
		filePath      string
		encoded       bool
		updateOnly    bool
	)

	secretCmd := &cli.Command{
		Use:     "set",
		Short:   "Register secret with optimus",
		Example: "optimus secret set <secret_name> <secret_value>",
		Long: `
This operation takes secret name as its first argument. 
Secret value can be either provided in second argument or through file flag. 
Use base64 flag if the value has been encoded.
		`,
	}
	secretCmd.Flags().StringVarP(&projectName, "project", "p", conf.GetProject().Name, "Project name of optimus managed repository")
	secretCmd.Flags().StringVarP(&namespaceName, "namespace", "n", conf.GetNamespace().Name, "Namespace of deployee")
	secretCmd.Flags().BoolVar(&encoded, "base64", false, "Create secret with value that has been encoded")
	secretCmd.Flags().BoolVar(&updateOnly, "update-only", false, "Only update existing secret, do not create new")
	secretCmd.Flags().StringVarP(&filePath, "file", "f", filePath, "Provide file path to create secret from file instead")

	secretCmd.RunE = func(cmd *cli.Command, args []string) error {
		secretName, err := getSecretName(args)
		if err != nil {
			return err
		}

		secretValue, err := getSecretValue(args, filePath, encoded)
		if err != nil {
			return err
		}

		registerSecretReq := &pb.RegisterSecretRequest{
			ProjectName:   projectName,
			SecretName:    secretName,
			Value:         secretValue,
			NamespaceName: namespaceName,
			UpdateOnly:    updateOnly,
		}
		return registerSecret(l, conf, registerSecretReq)
	}
	return secretCmd
}

func getSecretName(args []string) (string, error) {
	if len(args) < 1 {
		return "", errors.New("secret name is required")
	}
	if strings.HasPrefix(args[0], models.SecretTypeSystemDefinedPrefix) {
		return "", errors.New(fmt.Sprintf("secret name cannot be started with %s", models.SecretTypeSystemDefinedPrefix))
	}
	return args[0], nil
}

func getSecretValue(args []string, filePath string, encoded bool) (string, error) {
	var secretValue string
	if filePath == "" {
		if len(args) < 2 {
			return "", errors.New("secret value is required")
		}
		secretValue = args[1]
	} else {
		secretValueBytes, err := ioutil.ReadFile(filePath)
		if err != nil {
			return "", errors.Wrapf(err, "failed when reading secret file %s", filePath)
		}
		secretValue = string(secretValueBytes)
	}

	if !encoded {
		return base64.StdEncoding.EncodeToString([]byte(secretValue)), nil
	} else {
		if err := validateProperlyEncoded(secretValue); err != nil {
			return "", err
		}
		return secretValue, nil
	}
}

func validateProperlyEncoded(secretValue string) error {
	if _, err := base64.StdEncoding.DecodeString(secretValue); err != nil {
		return errors.New("value is not encoded. please use --base64 to let Optimus encode the secret for you.")
	}
	return nil
}

func registerSecret(l log.Logger, conf config.Provider, req *pb.RegisterSecretRequest) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, conf.GetHost()); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("can't reach optimus service")
		}
		return err
	}
	defer conn.Close()

	secretRequestTimeout, secretRequestCancel := context.WithTimeout(context.Background(), secretTimeout)
	defer secretRequestCancel()

	l.Info("please wait...")
	runtime := pb.NewRuntimeServiceClient(conn)

	registerSecretResponse, err := runtime.RegisterSecret(secretRequestTimeout, req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("secret registration took too long, timing out")
		}
		return errors.Wrapf(err, "request failed for creating secret %s", req.SecretName)
	}

	if registerSecretResponse.Success {
		l.Info("secret registered")
	} else {
		return errors.New(fmt.Sprintf("request failed for creating secret %s: %s", req.SecretName,
			registerSecretResponse.Message))
	}

	return nil
}
