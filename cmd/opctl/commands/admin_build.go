package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	v1handler "github.com/odpf/optimus/api/handler/v1"
	pb "github.com/odpf/optimus/api/proto/v1"
	"github.com/odpf/optimus/instance"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
)

var (
	assetOutputDir string
	scheduledAt    string
	taskType       string
	taskName       string

	taskInputDirectory  = "in"
	taskOutputDirectory = "out"

	writeToFileFn = utils.WriteStringToFileIndexed()
)

func adminBuildInstanceCommand(l logger) *cli.Command {
	cmd := &cli.Command{
		Use:     "instance",
		Short:   "Builds a Job instance including the assets for a scheduled execution",
		Example: "opctl admin build instance sample_replace --project \"project-id\" --output-dir /tmp",
		Args:    cli.MinimumNArgs(1),
	}

	cmd.Flags().StringVar(&assetOutputDir, "output-dir", "", "output directory for assets")
	cmd.MarkFlagRequired("output-dir")
	cmd.Flags().StringVar(&scheduledAt, "scheduled-at", "", "time at which the job was scheduled for execution")
	cmd.MarkFlagRequired("scheduled-at")
	cmd.Flags().StringVar(&taskType, "type", "", "type of task, could be base/hook")
	cmd.MarkFlagRequired("type")
	cmd.Flags().StringVar(&taskName, "name", "", "name of task, could be bq2bq/transporter/predator")
	cmd.MarkFlagRequired("name")

	cmd.Flags().StringVar(&projectName, "project", "", "name of the tenant")
	cmd.MarkFlagRequired("project")
	cmd.Flags().StringVar(&optimusHost, "host", "", "optimus service endpoint url")
	cmd.MarkFlagRequired("host")

	cmd.Run = func(c *cli.Command, args []string) {
		jobName := args[0]
		l.Printf("requesting resources for project %s, job %s at %s\nplease wait...\n", projectName, jobName, optimusHost)

		// append base path to input file directory
		inputDirectory := filepath.Join(assetOutputDir, taskInputDirectory)

		if err := getInstanceBuildRequest(l, jobName, inputDirectory); err != nil {
			l.Print(err)
			l.Print(errRequestFail)
			os.Exit(1)
		}
	}
	return cmd
}

// getInstanceBuildRequest fetches a JobRun from the store (eg, postgres)
// Based on the response, it builds assets like query, env and config
// for the Job Run which is saved into output files.
func getInstanceBuildRequest(l logger, jobName, inputDirectory string) (err error) {

	jobScheduledTime, err := time.Parse(models.InstanceScheduledAtTimeLayout, scheduledAt)
	if err != nil {
		return errors.Wrapf(err, "invalid time format, please use %s", models.InstanceScheduledAtTimeLayout)
	}
	jobScheduledTimeProto, err := ptypes.TimestampProto(jobScheduledTime)
	if err != nil {
		return errors.Wrapf(err, "unable to parse timestamp to proto %s", jobScheduledTime.String())
	}

	var conn *grpc.ClientConn
	if conn, err = createConnection(optimusHost); err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	adapt := v1handler.NewAdapter(models.TaskRegistry, models.HookRegistry)

	// fetch Instance by calling the optimus API
	jobResponse, err := runtime.RegisterInstance(ctx, &pb.RegisterInstanceRequest{
		ProjectName: projectName,
		JobName:     jobName,
		Type:        taskType,
		ScheduledAt: jobScheduledTimeProto,
	})
	if err != nil {
		return errors.Wrapf(err, "request failed for job %s", jobName)
	}

	jobSpec, err := adapt.FromJobProto(jobResponse.GetJob())
	if err != nil {
		return errors.Wrapf(err, "failed to parse job %s", jobName)
	}

	// make sure output dir exists
	if err := os.MkdirAll(inputDirectory, 0777); err != nil {
		return errors.Wrapf(err, "failed to create directory at %s", inputDirectory)
	}

	project := adapt.FromProjectProto(jobResponse.GetProject())
	instanceSpec, err := adapt.FromInstanceProto(jobResponse.GetInstance())
	if err != nil {
		return err
	}
	envMap, fileMap, err := instance.NewFeatureManager(project, jobSpec, instanceSpec).Generate(taskType, taskName)
	if err != nil {
		return err
	}

	// write all files in the fileMap to respective files
	for fileName, fileContent := range fileMap {
		filePath := filepath.Join(inputDirectory, fileName)
		if err := writeToFileFn(filePath, fileContent, l.Writer()); err != nil {
			return errors.Wrapf(err, "failed to write asset file at %s", filePath)
		}
	}

	// write all env into a file
	envFileBlob := ""
	for key, val := range envMap {
		envFileBlob += fmt.Sprintf("%s=\"%s\"\n", key, val)
	}
	filePath := filepath.Join(inputDirectory, models.InstanceDataTypeEnvFileName)
	if err := writeToFileFn(filePath, envFileBlob, l.Writer()); err != nil {
		return errors.Wrapf(err, "failed to write asset file at %s", filePath)
	}

	return nil
}
