package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func backupCreateCommand(l log.Logger, conf config.ProjectConfig, datastoreRepo models.DatastoreRepo) *cli.Command {
	var (
		backupCmd = &cli.Command{
			Use:     "create",
			Short:   "Create a backup",
			Example: "optimus backup create --resource <sample_resource_name>",
		}
		namespaceName    string
		dryRun           = false
		ignoreDownstream = false
		allDownstream    = false
		skipConfirm      = false
		resourceName     string
		description      string
		storerName       string
	)
	backupCmd.Flags().StringVarP(&namespaceName, "namespace", "n", namespaceName, "Namespace of the resource within project")
	backupCmd.MarkFlagRequired("namespace")

	backupCmd.Flags().StringVarP(&resourceName, "resource", "r", resourceName, "Resource name created inside the datastore")
	backupCmd.Flags().StringVarP(&description, "description", "i", description, "Describe intention to help identify the backup")
	backupCmd.Flags().StringVarP(&storerName, "datastore", "s", storerName, "Datastore type where the resource belongs")

	backupCmd.Flags().BoolVarP(&dryRun, "dry-run", "d", dryRun, "Only do a trial run with no permanent changes")
	backupCmd.Flags().BoolVar(&skipConfirm, "confirm", skipConfirm, "Skip asking for confirmation")
	backupCmd.Flags().BoolVarP(&allDownstream, "all-downstream", "", allDownstream, "Run backup for all downstreams across namespaces")
	backupCmd.Flags().BoolVar(&ignoreDownstream, "ignore-downstream", ignoreDownstream, "Do not take backups for dependent downstream resources")

	backupCmd.RunE = func(cmd *cli.Command, args []string) error {
		namespace, err := conf.GetNamespaceByName(namespaceName)
		if err != nil {
			return err
		}
		if storerName, err = extractDatastoreName(datastoreRepo, storerName); err != nil {
			return err
		}
		if resourceName, err = extractResourceName(resourceName); err != nil {
			return err
		}
		if description, err = extractDescription(description); err != nil {
			return err
		}

		var allowedDownstreamNamespaces []string
		if !ignoreDownstream {
			if allDownstream {
				allowedDownstreamNamespaces = []string{"*"}
			} else {
				allowedDownstreamNamespaces = []string{namespaceName}
			}
		}

		backupDryRunRequest := &pb.BackupDryRunRequest{
			ProjectName:                 conf.Project.Name,
			NamespaceName:               namespaceName,
			ResourceName:                resourceName,
			DatastoreName:               storerName,
			Description:                 description,
			AllowedDownstreamNamespaces: allowedDownstreamNamespaces,
		}
		if err := runBackupDryRunRequest(l, conf.Host, backupDryRunRequest, !ignoreDownstream); err != nil {
			l.Info(coloredNotice("Failed to run backup dry run"))
			return err
		}
		if dryRun {
			//if only dry run, exit now
			return nil
		}

		if !skipConfirm {
			proceedWithBackup := AnswerYes
			if err := survey.AskOne(&survey.Select{
				Message: "Proceed with backup?",
				Options: []string{AnswerYes, AnswerNo},
				Default: AnswerNo,
			}, &proceedWithBackup); err != nil {
				return err
			}
			if proceedWithBackup == AnswerNo {
				l.Info(coloredNotice("Aborting..."))
				return nil
			}
		}

		backupRequest := &pb.CreateBackupRequest{
			ProjectName:                 conf.Project.Name,
			NamespaceName:               namespaceName,
			ResourceName:                resourceName,
			DatastoreName:               storerName,
			Description:                 description,
			AllowedDownstreamNamespaces: allowedDownstreamNamespaces,
		}
		for _, ds := range namespace.Datastore {
			if ds.Type == storerName {
				backupRequest.Config = ds.Backup
			}
		}
		return runBackupRequest(l, conf.Host, backupRequest)
	}
	return backupCmd
}

func extractDatastoreName(datastoreRepo models.DatastoreRepo, storerName string) (string, error) {
	availableStorer := []string{}
	for _, s := range datastoreRepo.GetAll() {
		availableStorer = append(availableStorer, s.Name())
	}
	if storerName == "" {
		if err := survey.AskOne(&survey.Select{
			Message: "Select supported datastore?",
			Options: availableStorer,
		}, &storerName); err != nil {
			return "", err
		}
	}
	storerName = strings.ToLower(storerName)
	validStore := false
	for _, s := range availableStorer {
		if s == storerName {
			validStore = true
		}
	}
	if !validStore {
		return "", fmt.Errorf("invalid datastore type, available values are: %v", availableStorer)
	}
	return storerName, nil
}

func extractResourceName(resourceName string) (string, error) {
	if resourceName == "" {
		if err := survey.AskOne(&survey.Input{
			Message: "What is the resource name?",
			Help:    "Input urn of the resource",
		}, &resourceName, survey.WithValidator(survey.ComposeValidators(validateNoSlash, survey.MinLength(3),
			survey.MaxLength(1024)))); err != nil {
			return "", err
		}
	}
	return resourceName, nil
}

func extractDescription(description string) (string, error) {
	if description == "" {
		if err := survey.AskOne(&survey.Input{
			Message: "Why is this backup needed?",
			Help:    "Describe intention to help identify the backup",
		}, &description, survey.WithValidator(survey.ComposeValidators(survey.MinLength(3)))); err != nil {
			return "", err
		}
	}
	return description, nil
}

func runBackupDryRunRequest(l log.Logger, host string, backupRequest *pb.BackupDryRunRequest, backupDownstream bool) (err error) {
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

	requestTimeoutCtx, requestCancel := context.WithTimeout(context.Background(), backupTimeout)
	defer requestCancel()

	runtime := pb.NewRuntimeServiceClient(conn)

	spinner := NewProgressBar()
	spinner.Start("please wait...")
	backupDryRunResponse, err := runtime.BackupDryRun(requestTimeoutCtx, backupRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Backup dry run took too long, timing out"))
		}
		return fmt.Errorf("request failed to backup %s: %w", backupRequest.ResourceName, err)
	}

	printBackupDryRunResponse(l, backupRequest, backupDryRunResponse, backupDownstream)
	return nil
}

func runBackupRequest(l log.Logger, host string, backupRequest *pb.CreateBackupRequest) (err error) {
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

	requestTimeout, requestCancel := context.WithTimeout(context.Background(), backupTimeout)
	defer requestCancel()

	runtime := pb.NewRuntimeServiceClient(conn)

	spinner := NewProgressBar()
	spinner.Start("please wait...")
	backupResponse, err := runtime.CreateBackup(requestTimeout, backupRequest)
	spinner.Stop()

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Backup took too long, timing out"))
		}
		return fmt.Errorf("request failed to backup job %s: %w", backupRequest.ResourceName, err)
	}

	printBackupResponse(l, backupResponse)
	return nil
}

func printBackupResponse(l log.Logger, backupResponse *pb.CreateBackupResponse) {
	l.Info(coloredSuccess("Resource backup completed successfully:"))
	for counter, result := range backupResponse.Urn {
		l.Info(fmt.Sprintf("%d. %s", counter+1, result))
	}
}

func printBackupDryRunResponse(l log.Logger, backupRequest *pb.BackupDryRunRequest, backupResponse *pb.BackupDryRunResponse,
	backupDownstream bool) {
	if !backupDownstream {
		l.Info(coloredNotice(fmt.Sprintf("\nBackup list for %s. Downstreams will be ignored.", backupRequest.ResourceName)))
	} else {
		l.Info(coloredNotice(fmt.Sprintf("\nBackup list for %s. Supported downstreams will be included.", backupRequest.ResourceName)))
	}
	for counter, resource := range backupResponse.ResourceName {
		l.Info(fmt.Sprintf("%d. %s", counter+1, resource))
	}

	if len(backupResponse.IgnoredResources) > 0 {
		l.Info("\nThese resources will be ignored:")
	}
	for counter, ignoredResource := range backupResponse.IgnoredResources {
		l.Info(fmt.Sprintf("%d. %s", counter+1, ignoredResource))
	}
	l.Info("")
}
