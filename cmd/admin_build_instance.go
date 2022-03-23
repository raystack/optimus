package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/odpf/optimus/config"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	taskInputDirectory        = "in"
	adminBuildInstanceTimeout = time.Minute * 1
)

const unsubstitutedValue = "<no value>"

func adminBuildInstanceCommand(l log.Logger, conf config.ProjectConfig) *cli.Command {
	var (
		optimusHost    = conf.Host
		projectName    = conf.Project.Name
		assetOutputDir = "/tmp/"
		runType        = "task"
		runName        string
		scheduledAt    string
		cmd            = &cli.Command{
			Use:     "instance",
			Short:   "Builds a Job instance including the assets for a scheduled execution",
			Example: "optimus admin build instance <sample_replace> --output-dir </tmp> --scheduled-at <2021-01-14T02:00:00+00:00> --type task --name <bq2bq> [--project \"project-id\"]",
			Args:    cli.MinimumNArgs(1),
			Annotations: map[string]string{
				"group:core": "true",
			},
		}
	)
	cmd.Flags().StringVar(&assetOutputDir, "output-dir", assetOutputDir, "Output directory for assets")
	cmd.MarkFlagRequired("output-dir")
	cmd.Flags().StringVar(&scheduledAt, "scheduled-at", "", "Time at which the job was scheduled for execution")
	cmd.MarkFlagRequired("scheduled-at")
	cmd.Flags().StringVar(&runType, "type", "task", "Type of instance, could be task/hook")
	cmd.MarkFlagRequired("type")
	cmd.Flags().StringVar(&runName, "name", "", "Name of running instance, e.g., bq2bq/transporter/predator")
	cmd.MarkFlagRequired("name")

	cmd.Flags().StringVarP(&projectName, "project", "p", projectName, "Name of the optimus project")
	cmd.Flags().StringVar(&optimusHost, "host", optimusHost, "Optimus service endpoint url")

	cmd.RunE = func(c *cli.Command, args []string) error {
		jobName := args[0]
		l.Info(fmt.Sprintf("Requesting resources for project %s, job %s at %s", projectName, jobName, optimusHost))
		l.Info(fmt.Sprintf("Run name %s, run type %s, scheduled at %s\n", runName, runType, scheduledAt))
		l.Info("please wait...")

		// append base path to input file directory
		inputDirectory := filepath.Join(assetOutputDir, taskInputDirectory)
		return getInstanceBuildRequest(l, jobName, inputDirectory, optimusHost, projectName, scheduledAt, runType, runName)
	}
	return cmd
}

// getInstanceBuildRequest fetches a JobRun from the store (eg, postgres)
// Based on the response, it builds assets like query, env and config
// for the Job Run which is saved into output files.
func getInstanceBuildRequest(l log.Logger, jobName, inputDirectory, host, projectName, scheduledAt, runType, runName string) (err error) {
	jobScheduledTime, err := time.Parse(models.InstanceScheduledAtTimeLayout, scheduledAt)
	if err != nil {
		return fmt.Errorf("invalid time format, please use %s: %w", models.InstanceScheduledAtTimeLayout, err)
	}
	jobScheduledTimeProto := timestamppb.New(jobScheduledTime)

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

	timeoutCtx, cancel := context.WithTimeout(context.Background(), adminBuildInstanceTimeout)
	defer cancel()

	// fetch Instance by calling the optimus API
	runtime := pb.NewRuntimeServiceClient(conn)
	jobResponse, err := runtime.RegisterInstance(timeoutCtx, &pb.RegisterInstanceRequest{
		ProjectName:  projectName,
		JobName:      jobName,
		ScheduledAt:  jobScheduledTimeProto,
		InstanceType: pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[utils.ToEnumProto(runType, "type")]),
		InstanceName: runName,
	})
	if err != nil {
		return fmt.Errorf("request failed for job %s: %w", jobName, err)
	}

	// make sure output dir exists
	if err := os.MkdirAll(inputDirectory, 0777); err != nil {
		return fmt.Errorf("failed to create directory at %s: %w", inputDirectory, err)
	}
	writeToFileFn := utils.WriteStringToFileIndexed()

	// write all files in the fileMap to respective files
	for fileName, fileContent := range jobResponse.Context.Files {
		filePath := filepath.Join(inputDirectory, fileName)
		if err := writeToFileFn(filePath, fileContent, l.Writer()); err != nil {
			return fmt.Errorf("failed to write asset file at %s: %w", filePath, err)
		}
	}

	var keysWithUnsubstitutedValue []string

	// write all env into a file
	envFileBlob := ""
	for key, val := range jobResponse.Context.Envs {
		if strings.Contains(val, unsubstitutedValue) {
			keysWithUnsubstitutedValue = append(keysWithUnsubstitutedValue, key)
		}
		envFileBlob += fmt.Sprintf("%s='%s'\n", key, val)
	}
	filePath := filepath.Join(inputDirectory, models.InstanceDataTypeEnvFileName)
	if err := writeToFileFn(filePath, envFileBlob, l.Writer()); err != nil {
		return fmt.Errorf("failed to write asset file at %s: %w", filePath, err)
	}

	// write all secrets into a file
	secretsFileContent := ""
	for key, val := range jobResponse.Context.Secrets {
		if strings.Contains(val, unsubstitutedValue) {
			keysWithUnsubstitutedValue = append(keysWithUnsubstitutedValue, key)
		}
		secretsFileContent += fmt.Sprintf("%s='%s'\n", key, val)
	}
	secretsFilePath := filepath.Join(inputDirectory, models.InstanceDataTypeSecretFileName)
	if err := writeToFileFn(secretsFilePath, secretsFileContent, l.Writer()); err != nil {
		return fmt.Errorf("failed to write asset file at %s: %w", filePath, err)
	}

	if len(keysWithUnsubstitutedValue) > 0 {
		l.Warn(coloredNotice(fmt.Sprintf("Value not substituted for keys:\n%s", strings.Join(keysWithUnsubstitutedValue, "\n"))))
	}

	return nil
}
