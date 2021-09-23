package cmd

import (
	"context"
	"fmt"

	"github.com/AlecAivazis/survey/v2"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func backupCommand(l log.Logger, datastoreRepo models.DatastoreRepo, conf config.Provider) *cli.Command {
	cmd := &cli.Command{
		Use:   "backup",
		Short: "Backup a resource and its downstream",
		Long:  "Backup supported resource of a datastore and all of its downstream dependencies",
	}
	cmd.AddCommand(backupResourceSubCommand(l, datastoreRepo, conf))
	return cmd
}

func backupResourceSubCommand(l log.Logger, datastoreRepo models.DatastoreRepo, conf config.Provider) *cli.Command {
	backupCmd := &cli.Command{
		Use:   "resource",
		Short: "backup a resource",
	}

	var (
		project   string
		namespace string
		dryRun    bool
	)

	backupCmd.Flags().BoolVarP(&dryRun, "dry-run", "", dryRun, "do a trial run with no permanent changes")
	backupCmd.Flags().StringVarP(&project, "project", "p", "", "project name of optimus managed repository")
	backupCmd.MarkFlagRequired("project")
	backupCmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace of the requester")
	backupCmd.MarkFlagRequired("namespace")

	backupCmd.RunE = func(cmd *cli.Command, args []string) error {
		availableStorer := []string{}
		for _, s := range datastoreRepo.GetAll() {
			availableStorer = append(availableStorer, s.Name())
		}
		var storerName string
		if err := survey.AskOne(&survey.Select{
			Message: "Select supported datastores?",
			Options: availableStorer,
		}, &storerName); err != nil {
			return err
		}

		var qs = []*survey.Question{
			{
				Name: "name",
				Prompt: &survey.Input{
					Message: "What is the resource name?",
					Help:    "Input urn of the resource",
				},
				Validate: survey.ComposeValidators(validateNoSlash, survey.MinLength(3),
					survey.MaxLength(1024)),
			},
			{
				Name: "description",
				Prompt: &survey.Input{
					Message: "Why is this backup needed?",
					Help:    "Describe intention to help identify the backup",
				},
				Validate: survey.ComposeValidators(survey.MinLength(3)),
			},
			{
				Name: "backupDownstream",
				Prompt: &survey.Confirm{
					Message: "Backup downstream?",
					Help:    "Select yes to also backup the downstream resources",
				},
			},
		}
		inputs := map[string]interface{}{}
		if err := survey.Ask(qs, &inputs); err != nil {
			return err
		}
		resourceName := inputs["name"].(string)
		description := inputs["description"].(string)
		backupDownstream := inputs["backupDownstream"].(bool)
		backupRequest := &pb.BackupDryRunRequest{
			ProjectName:      project,
			Namespace:        namespace,
			ResourceName:     resourceName,
			DatastoreName:    storerName,
			Description:      description,
			IgnoreDownstream: !backupDownstream,
		}

		return runBackupDryRunRequest(l, conf, backupRequest)
	}
	return backupCmd
}

func runBackupDryRunRequest(l log.Logger, conf config.Provider, backupRequest *pb.BackupDryRunRequest) (err error) {
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

	requestTimeoutCtx, requestCancel := context.WithTimeout(context.Background(), replayTimeout)
	defer requestCancel()

	l.Info("please wait...")
	runtime := pb.NewRuntimeServiceClient(conn)

	backupDryRunResponse, err := runtime.BackupDryRun(requestTimeoutCtx, backupRequest)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("backup dry run took too long, timing out")
		}
		return errors.Wrapf(err, "request failed to backup job %s", backupRequest.ResourceName)
	}

	printBackupDryRunResponse(l, backupRequest, backupDryRunResponse)
	return nil
}

func printBackupDryRunResponse(l log.Logger, backupRequest *pb.BackupDryRunRequest, backupResponse *pb.BackupDryRunResponse) {
	if backupRequest.IgnoreDownstream {
		l.Info(coloredPrint(fmt.Sprintf("Backup list for %s. Downstreams will be ignored.", backupRequest.ResourceName)))
	} else {
		l.Info(coloredPrint(fmt.Sprintf("Backup list for %s. Supported downstreams will be included.", backupRequest.ResourceName)))
	}
	counter := 1
	for _, resource := range backupResponse.ResourceName {
		l.Info(fmt.Sprintf("%d. %s", counter, resource))
		counter++
	}
}
