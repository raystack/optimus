package cmd

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
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
	cmd.AddCommand(secretListSubCommand(l, conf))
	return cmd
}

func secretSetSubCommand(l log.Logger, conf config.Provider) *cli.Command {
	var (
		projectName   string
		namespaceName string
		filePath      string
		encoded       bool
		updateOnly    bool
		skipConfirm   bool
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
	secretCmd.Flags().BoolVar(&skipConfirm, "confirm", false, "Skip asking for confirmation")

	secretCmd.RunE = func(cmd *cli.Command, args []string) error {
		secretName, err := getSecretName(args)
		if err != nil {
			return err
		}

		secretValue, err := getSecretValue(args, filePath, encoded)
		if err != nil {
			return err
		}

		if updateOnly {
			updateSecretRequest := &pb.UpdateSecretRequest{
				ProjectName:   projectName,
				SecretName:    secretName,
				Value:         secretValue,
				NamespaceName: namespaceName,
			}
			return updateSecret(l, conf, updateSecretRequest)
		}
		registerSecretReq := &pb.RegisterSecretRequest{
			ProjectName:   projectName,
			SecretName:    secretName,
			Value:         secretValue,
			NamespaceName: namespaceName,
		}
		err = registerSecret(l, conf, registerSecretReq)
		if err != nil {
			if status.Code(err) == codes.AlreadyExists {
				proceedWithUpdate := "Yes"
				if !skipConfirm {
					if err := survey.AskOne(&survey.Select{
						Message: "Secret already exists, proceed with update?",
						Options: []string{"Yes", "No"},
						Default: "No",
					}, &proceedWithUpdate); err != nil {
						return err
					}
				}
				if proceedWithUpdate == "Yes" {
					updateSecretRequest := &pb.UpdateSecretRequest{
						ProjectName:   projectName,
						SecretName:    secretName,
						Value:         secretValue,
						NamespaceName: namespaceName,
					}
					return updateSecret(l, conf, updateSecretRequest)
				} else {
					l.Info(coloredNotice("Aborting..."))
					return nil
				}
			} else {
				return errors.Wrapf(err, "request failed for creating secret %s", secretName)
			}
		}
		return nil
	}
	return secretCmd
}

func secretListSubCommand(l log.Logger, conf config.Provider) *cli.Command {
	var projectName string

	secretListCmd := &cli.Command{
		Use:     "list",
		Short:   "Show all the secrets registered with optimus",
		Example: "optimus secret list",
		Long:    `This operation shows the secrets for project.`,
	}
	secretListCmd.Flags().StringVarP(&projectName, "project", "p", conf.GetProject().Name, "Project name of optimus managed repository")

	secretListCmd.RunE = func(cmd *cli.Command, args []string) error {
		updateSecretRequest := &pb.ListSecretsRequest{
			ProjectName: projectName,
		}
		return listSecret(l, conf, updateSecretRequest)
	}
	return secretListCmd
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
		return errors.New("value is not encoded. please remove --base64 to let Optimus encode the secret for you.")
	}
	return nil
}

func registerSecret(l log.Logger, conf config.Provider, req *pb.RegisterSecretRequest) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, conf.GetHost()); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(conf.GetHost()).Error())
		}
		return err
	}
	defer conn.Close()

	secretRequestTimeout, secretRequestCancel := context.WithTimeout(context.Background(), secretTimeout)
	defer secretRequestCancel()

	spinner := NewProgressBar()
	spinner.Start("please wait...")
	runtime := pb.NewRuntimeServiceClient(conn)

	_, err = runtime.RegisterSecret(secretRequestTimeout, req)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Secret registration took too long, timing out"))
		}
		return err
	}

	l.Info(coloredSuccess("Secret registered"))

	return nil
}

func updateSecret(l log.Logger, conf config.Provider, req *pb.UpdateSecretRequest) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, conf.GetHost()); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(conf.GetHost()).Error())
		}
		return err
	}
	defer conn.Close()

	secretRequestTimeout, secretRequestCancel := context.WithTimeout(context.Background(), secretTimeout)
	defer secretRequestCancel()

	spinner := NewProgressBar()
	spinner.Start("please wait...")
	runtime := pb.NewRuntimeServiceClient(conn)

	_, err = runtime.UpdateSecret(secretRequestTimeout, req)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Secret update took too long, timing out"))
		}
		return errors.Wrapf(err, "request failed for updating secret %s", req.SecretName)
	}

	l.Info(coloredSuccess("Secret updated"))

	return nil
}

func listSecret(l log.Logger, conf config.Provider, req *pb.ListSecretsRequest) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, conf.GetHost()); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(conf.GetHost()).Error())
		}
		return err
	}
	defer conn.Close()

	secretRequestTimeout, secretRequestCancel := context.WithTimeout(context.Background(), secretTimeout)
	defer secretRequestCancel()

	spinner := NewProgressBar()
	spinner.Start("please wait...")
	runtime := pb.NewRuntimeServiceClient(conn)

	listSecretsResponse, err := runtime.ListSecrets(secretRequestTimeout, req)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Secret listing took too long, timing out"))
		}
		return errors.Wrap(err, "request failed for listing secrets")
	}

	if len(listSecretsResponse.Secrets) == 0 {
		l.Info(coloredNotice("No secrets were found in %s project.", req.ProjectName))
	} else {
		printListOfSecrets(l, req.ProjectName, listSecretsResponse)
	}

	return nil
}

func printListOfSecrets(l log.Logger, projectName string, listSecretsResponse *pb.ListSecretsResponse) {
	l.Info(coloredNotice("Secrets for project: %s", projectName))
	table := tablewriter.NewWriter(l.Writer())
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
	l.Info("")
}
