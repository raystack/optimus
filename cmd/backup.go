package cmd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"

	"github.com/AlecAivazis/survey/v2"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	backupTimeout = time.Minute * 15
)

func backupCommand(l log.Logger, datastoreRepo models.DatastoreRepo, conf config.Provider) *cli.Command {
	cmd := &cli.Command{
		Use:   "backup",
		Short: "Backup a resource and its downstream",
		Long:  "Backup supported resource of a datastore and all of its downstream dependencies",
	}
	cmd.AddCommand(backupResourceSubCommand(l, datastoreRepo, conf))
	cmd.AddCommand(backupListSubCommand(l, datastoreRepo, conf))
	cmd.AddCommand(backupDetailSubCommand(l, datastoreRepo, conf))
	return cmd
}

func backupResourceSubCommand(l log.Logger, datastoreRepo models.DatastoreRepo, conf config.Provider) *cli.Command {
	backupCmd := &cli.Command{
		Use:   "resource",
		Short: "backup a resource",
	}

	var (
		project       string
		namespace     string
		dryRun        bool
		allDownstream bool
	)

	backupCmd.Flags().BoolVarP(&dryRun, "dry-run", "", dryRun, "do a trial run with no permanent changes")
	backupCmd.Flags().StringVarP(&project, "project", "p", conf.GetProject().Name, "project name of optimus managed repository")
	backupCmd.Flags().StringVarP(&namespace, "namespace", "n", conf.GetNamespace().Name, "namespace of the requester")
	backupCmd.Flags().BoolVarP(&allDownstream, "all-downstream", "", allDownstream, "run replay for all downstream across namespaces")

	backupCmd.RunE = func(cmd *cli.Command, args []string) error {
		availableStorer := []string{}
		for _, s := range datastoreRepo.GetAll() {
			availableStorer = append(availableStorer, s.Name())
		}
		var storerName string
		if err := survey.AskOne(&survey.Select{
			Message: "Select supported datastore?",
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

		var allowedDownstreamNamespaces []string
		if backupDownstream {
			if allDownstream {
				allowedDownstreamNamespaces = []string{"*"}
			} else {
				allowedDownstreamNamespaces = []string{namespace}
			}
		}

		backupDryRunRequest := &pb.BackupDryRunRequest{
			ProjectName:                 project,
			Namespace:                   namespace,
			ResourceName:                resourceName,
			DatastoreName:               storerName,
			Description:                 description,
			AllowedDownstreamNamespaces: allowedDownstreamNamespaces,
		}

		if err := runBackupDryRunRequest(l, conf, backupDryRunRequest, backupDownstream); err != nil {
			l.Info("unable to run backup dry run")
			return err
		}

		if dryRun {
			//if only dry run, exit now
			return nil
		}

		proceedWithBackup := "Yes"
		if err := survey.AskOne(&survey.Select{
			Message: "Proceed the backup?",
			Options: []string{"Yes", "No"},
			Default: "Yes",
		}, &proceedWithBackup); err != nil {
			return err
		}

		if proceedWithBackup == "No" {
			l.Info("aborting...")
			return nil
		}

		backupRequest := &pb.CreateBackupRequest{
			ProjectName:                 project,
			Namespace:                   namespace,
			ResourceName:                resourceName,
			DatastoreName:               storerName,
			Description:                 description,
			AllowedDownstreamNamespaces: allowedDownstreamNamespaces,
		}

		for _, ds := range conf.GetDatastore() {
			if ds.Type == storerName {
				backupRequest.Config = ds.Backup
			}
		}

		return runBackupRequest(l, conf, backupRequest)
	}
	return backupCmd
}

func runBackupDryRunRequest(l log.Logger, conf config.Provider, backupRequest *pb.BackupDryRunRequest, backupDownstream bool) (err error) {
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

	requestTimeoutCtx, requestCancel := context.WithTimeout(context.Background(), backupTimeout)
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

	printBackupDryRunResponse(l, backupRequest, backupDryRunResponse, backupDownstream)
	return nil
}

func runBackupRequest(l log.Logger, conf config.Provider, backupRequest *pb.CreateBackupRequest) (err error) {
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

	requestTimeout, requestCancel := context.WithTimeout(context.Background(), backupTimeout)
	defer requestCancel()

	l.Info("please wait...")
	runtime := pb.NewRuntimeServiceClient(conn)

	backupResponse, err := runtime.CreateBackup(requestTimeout, backupRequest)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("backup took too long, timing out")
		}
		return errors.Wrapf(err, "request failed to backup job %s", backupRequest.ResourceName)
	}

	printBackupResponse(l, backupResponse)
	return nil
}

func printBackupResponse(l log.Logger, backupResponse *pb.CreateBackupResponse) {
	l.Info(coloredSuccess("\nBackup Finished"))
	l.Info("Resource backup completed successfully:")
	counter := 1
	for _, result := range backupResponse.Urn {
		l.Info(fmt.Sprintf("%d. %s", counter, result))
		counter++
	}
}

func printBackupDryRunResponse(l log.Logger, backupRequest *pb.BackupDryRunRequest, backupResponse *pb.BackupDryRunResponse,
	backupDownstream bool) {
	if !backupDownstream {
		l.Info(coloredNotice(fmt.Sprintf("Backup list for %s. Downstreams will be ignored.", backupRequest.ResourceName)))
	} else {
		l.Info(coloredNotice(fmt.Sprintf("Backup list for %s. Supported downstreams will be included.", backupRequest.ResourceName)))
	}
	counter := 1
	for _, resource := range backupResponse.ResourceName {
		l.Info(fmt.Sprintf("%d. %s", counter, resource))
		counter++
	}

	if len(backupResponse.IgnoredResources) > 0 {
		l.Info(coloredPrint("\nThese resources will be ignored."))
	}
	counter = 1
	for _, ignoredResource := range backupResponse.IgnoredResources {
		l.Info(fmt.Sprintf("%d. %s", counter, ignoredResource))
		counter++
	}
}

func backupListSubCommand(l log.Logger, datastoreRepo models.DatastoreRepo, conf config.Provider) *cli.Command {
	backupCmd := &cli.Command{
		Use:   "list",
		Short: "get list of backup per project and datastore",
	}

	var (
		project string
	)

	backupCmd.Flags().StringVarP(&project, "project", "p", conf.GetProject().Name, "project name of optimus managed repository")

	backupCmd.RunE = func(cmd *cli.Command, args []string) error {
		availableStorer := []string{}
		for _, s := range datastoreRepo.GetAll() {
			availableStorer = append(availableStorer, s.Name())
		}
		var storerName string
		if err := survey.AskOne(&survey.Select{
			Message: "Select supported datastore?",
			Options: availableStorer,
		}, &storerName); err != nil {
			return err
		}

		listBackupsRequest := &pb.ListBackupsRequest{
			ProjectName:   project,
			DatastoreName: storerName,
		}

		dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
		defer dialCancel()

		conn, err := createConnection(dialTimeoutCtx, conf.GetHost())
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Info("can't reach optimus service")
			}
			return err
		}
		defer conn.Close()

		requestTimeout, requestCancel := context.WithTimeout(context.Background(), backupTimeout)
		defer requestCancel()

		l.Info("please wait...")
		runtime := pb.NewRuntimeServiceClient(conn)

		listBackupsResponse, err := runtime.ListBackups(requestTimeout, listBackupsRequest)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Info("getting list of backups took too long, timing out")
			}
			return errors.Wrapf(err, "request failed to get list backups")
		}

		if len(listBackupsResponse.Backups) == 0 {
			l.Info(fmt.Sprintf("no backups were found in %s project.", project))
		} else {
			printBackupListResponse(l, listBackupsResponse)
		}
		return nil
	}
	return backupCmd
}

func printBackupListResponse(l log.Logger, listBackupsResponse *pb.ListBackupsResponse) {
	l.Info(coloredNotice("Latest Backups"))
	table := tablewriter.NewWriter(l.Writer())
	table.SetBorder(false)
	table.SetHeader([]string{
		"ID",
		"Resource",
		"Created",
		"Ignore Downstream?",
		"TTL",
		"Description",
	})

	for _, backupSpec := range listBackupsResponse.Backups {
		ignoreDownstream := backupSpec.Config[models.ConfigIgnoreDownstream]
		ttl := backupSpec.Config[models.ConfigTTL]
		table.Append([]string{
			backupSpec.Id,
			backupSpec.ResourceName,
			backupSpec.CreatedAt.AsTime().Format(time.RFC3339),
			ignoreDownstream,
			ttl,
			backupSpec.Description,
		})
	}

	table.Render()
}

func backupDetailSubCommand(l log.Logger, datastoreRepo models.DatastoreRepo, conf config.Provider) *cli.Command {
	backupCmd := &cli.Command{
		Use:   "detail",
		Short: "get backup detail using uuid and datastore",
	}

	var (
		project string
	)

	backupCmd.Flags().StringVarP(&project, "project", "p", conf.GetProject().Name, "project name of optimus managed repository")

	backupCmd.RunE = func(cmd *cli.Command, args []string) error {
		availableStorer := []string{}
		for _, s := range datastoreRepo.GetAll() {
			availableStorer = append(availableStorer, s.Name())
		}
		var storerName string
		if err := survey.AskOne(&survey.Select{
			Message: "Select supported datastore?",
			Options: availableStorer,
		}, &storerName); err != nil {
			return err
		}

		listBackupsRequest := &pb.GetBackupRequest{
			ProjectName:   project,
			DatastoreName: storerName,
			Id:            args[0],
		}

		dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
		defer dialCancel()

		conn, err := createConnection(dialTimeoutCtx, conf.GetHost())
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Info("can't reach optimus service")
			}
			return err
		}
		defer conn.Close()

		requestTimeout, requestCancel := context.WithTimeout(context.Background(), backupTimeout)
		defer requestCancel()

		l.Info("please wait...")
		runtime := pb.NewRuntimeServiceClient(conn)

		backupDetailResponse, err := runtime.GetBackup(requestTimeout, listBackupsRequest)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Info("getting backup detail took too long, timing out")
			}
			return errors.Wrapf(err, "request failed to get backup detail")
		}

		printBackupDetailResponse(l, backupDetailResponse)
		return nil
	}
	return backupCmd
}

func printBackupDetailResponse(l log.Logger, backupDetailResponse *pb.GetBackupResponse) {
	table := tablewriter.NewWriter(l.Writer())
	table.SetBorder(false)

	var expiry time.Time
	ttl := backupDetailResponse.Spec.Config[models.ConfigTTL]
	if ttl != "" {
		ttlDuration, err := time.ParseDuration(ttl)
		if err != nil {
			l.Info("unable to parse backup TTL")
		}
		expiry = backupDetailResponse.Spec.CreatedAt.AsTime().Add(ttlDuration)
	}

	table.Append([]string{"ID", backupDetailResponse.Spec.Id})
	table.Append([]string{"Resource", backupDetailResponse.Spec.ResourceName})
	table.Append([]string{"Created", backupDetailResponse.Spec.CreatedAt.AsTime().Format(time.RFC3339)})
	table.Append([]string{"Ignore Downstream?", backupDetailResponse.Spec.Config[models.ConfigIgnoreDownstream]})
	table.Append([]string{"Expire at", expiry.Format(time.RFC3339)})
	table.Append([]string{"Description", backupDetailResponse.Spec.Description})
	table.Append([]string{"Result", strings.Join(backupDetailResponse.Urn[:], "\n")})

	table.Render()
}
