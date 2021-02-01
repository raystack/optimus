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
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
)

var (
	projectName    string
	assetOutputDir string
	scheduledAt    string
	runType        string

	taskInputDirectory  = "in"
	taskOutputDirectory = "out"

	writeToFileFn = utils.WriteStringToFileIndexed()
)

// adminCommand requests a resource from optimus
func adminCommand(l logger) *cli.Command {
	cmd := &cli.Command{
		Use:   "admin",
		Short: "administration commands, should not be used by user",
	}
	cmd.AddCommand(adminCreateCommand(l))
	return cmd
}

// adminCreateCommand requests a resource from optimus
func adminCreateCommand(l logger) *cli.Command {
	cmd := &cli.Command{
		Use:   "create", // TODO think of some meaning full command hierarchy
		Short: "Register a job run and get required assets",
	}
	cmd.AddCommand(getAdminCreateInstanceCommand(l))
	return cmd
}

func getAdminCreateInstanceCommand(l logger) *cli.Command {
	cmd := &cli.Command{
		Use:     "instance",
		Short:   "Get a Job instance including the assets for a scheduled execution",
		Example: "opctl admin create instance sample_replace --project \"project-id\" --output-dir /tmp",
		Args:    cli.MinimumNArgs(1),
	}
	cmd.Flags().StringVar(&projectName, "project", "", "project name of deployee")
	cmd.MarkFlagRequired("project")
	cmd.Flags().StringVar(&assetOutputDir, "output-dir", "", "output directory for assets")
	cmd.MarkFlagRequired("output-dir")
	cmd.Flags().StringVar(&scheduledAt, "scheduled-at", "", "output directory for assets")
	cmd.MarkFlagRequired("scheduled-at")
	cmd.Flags().StringVar(&runType, "type", "", "type of task, could be base/hook")
	cmd.MarkFlagRequired("type")
	cmd.Flags().StringVar(&deployHost, "host", "", "deployment service endpoint url")
	cmd.MarkFlagRequired("host")

	cmd.Run = func(c *cli.Command, args []string) {
		jobName := args[0]
		l.Printf("requesting resources for project %s, job %s at %s\nplease wait...\n", projectName, jobName, deployHost)

		// append base path to input file directory
		inputDirectory := filepath.Join(assetOutputDir, taskInputDirectory)

		if err := getInstanceCreateRequest(l, jobName, inputDirectory); err != nil {
			l.Print(err)
			l.Print(errDeployFail)
			os.Exit(1)
		}
	}
	return cmd
}

// getInstanceCreateRequest fetches a JobRun from the store (eg, postgres)
// Based on the response, it builds assets like query, env and config
// for the Job Run which is saved into output files.
func getInstanceCreateRequest(l logger, jobName, inputDirectory string) (err error) {

	jobScheduledTime, err := time.Parse(models.InstanceScheduledAtTimeLayout, scheduledAt)
	if err != nil {
		return errors.Wrapf(err, "invalid time format, please use %s", models.InstanceScheduledAtTimeLayout)
	}
	jobScheduledTimeProto, err := ptypes.TimestampProto(jobScheduledTime)
	if err != nil {
		return errors.Wrapf(err, "unable to parse timestamp to proto %s", jobScheduledTime.String())
	}

	var conn *grpc.ClientConn
	if conn, err = createConnection(deployHost); err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	adapt := v1handler.NewAdapter(models.TaskRegistry)

	// fetch Instance by calling the optimus API
	jobResponse, err := runtime.RegisterInstance(ctx, &pb.RegisterInstanceRequest{
		ProjectName: projectName,
		JobName:     jobName,
		Type:        runType,
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

	// write all asset files locally
	for _, jobAsset := range jobSpec.Assets.GetAll() {
		filePath := filepath.Join(inputDirectory, jobAsset.Name)
		if err := writeToFileFn(filePath, jobAsset.Value, l.Writer()); err != nil {
			return errors.Wrapf(err, "failed to write asset file at %s", filePath)
		}
	}

	// for collecting .env vars
	envMap := map[string]string{}
	for key, val := range jobSpec.Task.Config {
		// TODO(not unless we support config macro conversions):
		// templatize task config variables
		envMap[key] = val
	}

	// get the Data from stored JobRun and write it into files.
	if jobResponse.GetInstance().GetData() != nil {
		for _, jobRunData := range jobResponse.GetInstance().GetData() {
			switch jobRunData.Type {
			case models.InstanceDataTypeFile:
				filePath := filepath.Join(inputDirectory, jobRunData.Name)
				if err := writeToFileFn(filepath.Join(inputDirectory, jobRunData.Name), jobRunData.Value, l.Writer()); err != nil {
					return errors.Wrapf(err, "failed to write asset file at %s", filePath)
				}
			case models.InstanceDataTypeEnv:
				envMap[jobRunData.Name] = jobRunData.Value
			}
		}
	}

	// build .env file
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

// adminUpdateCommand requests a resource from optimus
// TODO
func adminUpdateCommand(l logger) *cli.Command {
	cmd := &cli.Command{
		Use:   "update",
		Short: "Update a job run with configs",
	}
	return cmd
}
