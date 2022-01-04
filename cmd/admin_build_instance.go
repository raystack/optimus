package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	taskInputDirectory = "in"

	adminBuildInstanceTimeout = time.Minute * 1
)

func adminBuildInstanceCommand(l log.Logger) *cli.Command {
	var (
		optimusHost    string
		projectName    string
		assetOutputDir string
		scheduledAt    string
		runType        string
		runName        string
	)
	cmd := &cli.Command{
		Use:     "instance",
		Short:   "Builds a Job instance including the assets for a scheduled execution",
		Example: "optimus admin build instance sample_replace --project \"project-id\" --output-dir /tmp",
		Args:    cli.MinimumNArgs(1),
	}

	cmd.Flags().StringVar(&assetOutputDir, "output-dir", "", "output directory for assets")
	cmd.MarkFlagRequired("output-dir")
	cmd.Flags().StringVar(&scheduledAt, "scheduled-at", "", "time at which the job was scheduled for execution")
	cmd.MarkFlagRequired("scheduled-at")
	cmd.Flags().StringVar(&runType, "type", "", "type of task, could be base/hook")
	cmd.MarkFlagRequired("type")
	cmd.Flags().StringVar(&runName, "name", "", "name of task, could be bq2bq/transporter/predator")
	cmd.MarkFlagRequired("name")

	cmd.Flags().StringVar(&projectName, "project", "", "name of the tenant")
	cmd.MarkFlagRequired("project")
	cmd.Flags().StringVar(&optimusHost, "host", "", "optimus service endpoint url")
	cmd.MarkFlagRequired("host")

	cmd.RunE = func(c *cli.Command, args []string) error {
		jobName := args[0]
		l.Info(fmt.Sprintf("requesting resources for project %s, job %s at %s\nplease wait...", projectName, jobName, optimusHost))
		l.Info(fmt.Sprintf("run name %s, run type %s, scheduled at %s", runName, runType, scheduledAt))
		// append base path to input file directory
		inputDirectory := filepath.Join(assetOutputDir, taskInputDirectory)

		if err := getInstanceBuildRequest(l, jobName, inputDirectory, optimusHost, projectName, scheduledAt, runType, runName); err != nil {
			return err
		}
		return nil
	}
	return cmd
}

// getInstanceBuildRequest fetches a JobRun from the store (eg, postgres)
// Based on the response, it builds assets like query, env and config
// for the Job Run which is saved into output files.
func getInstanceBuildRequest(l log.Logger, jobName, inputDirectory, host, projectName, scheduledAt, runType, runName string) (err error) {
	jobScheduledTime, err := time.Parse(models.InstanceScheduledAtTimeLayout, scheduledAt)
	if err != nil {
		return errors.Wrapf(err, "invalid time format, please use %s", models.InstanceScheduledAtTimeLayout)
	}
	jobScheduledTimeProto := timestamppb.New(jobScheduledTime)

	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("can't reach optimus service, timing out")
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
		InstanceType: pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[strings.ToUpper(runType)]),
		InstanceName: runName,
	})
	if err != nil {
		return errors.Wrapf(err, "request failed for job %s", jobName)
	}

	// make sure output dir exists
	if err := os.MkdirAll(inputDirectory, 0777); err != nil {
		return errors.Wrapf(err, "failed to create directory at %s", inputDirectory)
	}
	writeToFileFn := utils.WriteStringToFileIndexed()

	// write all files in the fileMap to respective files
	for fileName, fileContent := range jobResponse.Context.Files {
		filePath := filepath.Join(inputDirectory, fileName)
		if err := writeToFileFn(filePath, fileContent, l.Writer()); err != nil {
			return errors.Wrapf(err, "failed to write asset file at %s", filePath)
		}
	}

	// write all env into a file
	envFileBlob := ""
	for key, val := range jobResponse.Context.Envs {
		envFileBlob += fmt.Sprintf("%s='%s'\n", key, val)
	}
	filePath := filepath.Join(inputDirectory, models.InstanceDataTypeEnvFileName)
	if err := writeToFileFn(filePath, envFileBlob, l.Writer()); err != nil {
		return errors.Wrapf(err, "failed to write asset file at %s", filePath)
	}

	return nil
}
