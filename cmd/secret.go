package cmd

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/salt/log"
	"github.com/olekukonko/tablewriter"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

const (
	secretTimeout = time.Minute * 2
)

func secretCommand(l log.Logger, conf config.Optimus) *cli.Command {
	cmd := &cli.Command{
		Use:   "secret",
		Short: "Manage secrets to be used in jobs",
	}
	cmd.AddCommand(secretSetSubCommand(l, conf))
	cmd.AddCommand(secretListSubCommand(l, conf))
	cmd.AddCommand(secretDeleteSubCommand(l, conf))
	return cmd
}

func secretSetSubCommand(l log.Logger, conf config.Optimus) *cli.Command {
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
	secretCmd.Flags().StringVarP(&projectName, "project", "p", conf.Project.Name, "Project name of optimus managed repository")
	secretCmd.Flags().StringVarP(&namespaceName, "namespace", "n", namespaceName, "Namespace of deployee")
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
			return updateSecret(l, conf.Host, updateSecretRequest)
		}
		registerSecretReq := &pb.RegisterSecretRequest{
			ProjectName:   projectName,
			SecretName:    secretName,
			Value:         secretValue,
			NamespaceName: namespaceName,
		}
		err = registerSecret(l, conf.Host, registerSecretReq)
		if err != nil {
			if status.Code(err) == codes.AlreadyExists {
				proceedWithUpdate := AnswerYes
				if !skipConfirm {
					if err := survey.AskOne(&survey.Select{
						Message: "Secret already exists, proceed with update?",
						Options: []string{AnswerYes, AnswerNo},
						Default: AnswerNo,
					}, &proceedWithUpdate); err != nil {
						return err
					}
				}
				if proceedWithUpdate == AnswerYes {
					updateSecretRequest := &pb.UpdateSecretRequest{
						ProjectName:   projectName,
						SecretName:    secretName,
						Value:         secretValue,
						NamespaceName: namespaceName,
					}
					return updateSecret(l, conf.Host, updateSecretRequest)
				}
				l.Info(coloredNotice("Aborting..."))
				return nil
			}
			return fmt.Errorf("%w: request failed for creating secret %s", err, secretName)
		}
		return nil
	}
	return secretCmd
}

func secretListSubCommand(l log.Logger, conf config.Optimus) *cli.Command {
	var projectName string

	secretListCmd := &cli.Command{
		Use:     "list",
		Short:   "Show all the secrets registered with optimus",
		Example: "optimus secret list",
		Long:    `This operation shows the secrets for project.`,
	}
	secretListCmd.Flags().StringVarP(&projectName, "project", "p", conf.Project.Name, "Project name of optimus managed repository")

	secretListCmd.RunE = func(cmd *cli.Command, args []string) error {
		updateSecretRequest := &pb.ListSecretsRequest{
			ProjectName: projectName,
		}
		return listSecret(l, conf.Host, updateSecretRequest)
	}
	return secretListCmd
}

func secretDeleteSubCommand(l log.Logger, conf config.Optimus) *cli.Command {
	var projectName, namespaceName string

	cmd := &cli.Command{
		Use:     "delete",
		Short:   "Delete a secrets registered with optimus",
		Example: "optimus secret delete <secret_name>",
		Long:    `This operation deletes a secret registered with optimus.`,
	}
	cmd.Flags().StringVarP(&projectName, "project", "p", conf.Project.Name, "Project name of optimus managed repository")
	cmd.Flags().StringVarP(&namespaceName, "namespace", "n", namespaceName, "Namespace name of optimus managed repository")

	cmd.RunE = func(cmd *cli.Command, args []string) error {
		secretName, err := getSecretName(args)
		if err != nil {
			return err
		}

		deleteSecretRequest := &pb.DeleteSecretRequest{
			ProjectName:   projectName,
			SecretName:    secretName,
			NamespaceName: namespaceName,
		}
		return deleteSecret(l, conf.Host, deleteSecretRequest)
	}
	return cmd
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
	} else {
		if err := validateProperlyEncoded(secretValue); err != nil {
			return "", err
		}
		return secretValue, nil
	}
}

func validateProperlyEncoded(secretValue string) error {
	if _, err := base64.StdEncoding.DecodeString(secretValue); err != nil {
		return errors.New("value is not encoded, please remove --base64 to let Optimus encode the secret for you")
	}
	return nil
}

func registerSecret(l log.Logger, host string, req *pb.RegisterSecretRequest) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(host).Error())
		}
		return err
	}
	defer conn.Close()

	secretRequestTimeout, secretRequestCancel := context.WithTimeout(context.Background(), secretTimeout)
	defer secretRequestCancel()

	spinner := NewProgressBar()
	spinner.Start("please wait...")
	secret := pb.NewSecretServiceClient(conn)

	_, err = secret.RegisterSecret(secretRequestTimeout, req)
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

func updateSecret(l log.Logger, host string, req *pb.UpdateSecretRequest) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(host).Error())
		}
		return err
	}
	defer conn.Close()

	secretRequestTimeout, secretRequestCancel := context.WithTimeout(context.Background(), secretTimeout)
	defer secretRequestCancel()

	spinner := NewProgressBar()
	spinner.Start("please wait...")
	secret := pb.NewSecretServiceClient(conn)

	_, err = secret.UpdateSecret(secretRequestTimeout, req)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Secret update took too long, timing out"))
		}
		return fmt.Errorf("%w: request failed for updating secret %s", err, req.SecretName)
	}

	l.Info(coloredSuccess("Secret updated"))

	return nil
}

func deleteSecret(l log.Logger, host string, req *pb.DeleteSecretRequest) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(host).Error())
		}
		return err
	}
	defer conn.Close()

	secretRequestTimeout, secretRequestCancel := context.WithTimeout(context.Background(), secretTimeout)
	defer secretRequestCancel()

	spinner := NewProgressBar()
	spinner.Start("please wait...")
	secret := pb.NewSecretServiceClient(conn)

	_, err = secret.DeleteSecret(secretRequestTimeout, req)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Secret delete took too long, timing out"))
		}
		return fmt.Errorf("%w: request failed for deleting secret %s", err, req.SecretName)
	}

	l.Info(coloredSuccess("Secret deleted"))

	return nil
}

func listSecret(l log.Logger, host string, req *pb.ListSecretsRequest) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(host).Error())
		}
		return err
	}
	defer conn.Close()

	secretRequestTimeout, secretRequestCancel := context.WithTimeout(context.Background(), secretTimeout)
	defer secretRequestCancel()

	spinner := NewProgressBar()
	spinner.Start("please wait...")
	secret := pb.NewSecretServiceClient(conn)

	listSecretsResponse, err := secret.ListSecrets(secretRequestTimeout, req)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Secret listing took too long, timing out"))
		}
		return fmt.Errorf("%w: request failed for listing secrets", err)
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
