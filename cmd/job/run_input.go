package job

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
)

const (
	jobRunInputCompileAssetsTimeout = time.Minute * 1

	taskInputDirectory = "in"
	unsubstitutedValue = "<no value>"
)

type jobRunInputCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	assetOutputDir string
	runType        string
	runName        string
	scheduledAt    string

	keysWithUnsubstitutedValue []string
}

// NewJobRunInputCommand gets compiled assets required for a job run
func NewJobRunInputCommand(clientConfig *config.ClientConfig) *cobra.Command {
	jobRunInput := &jobRunInputCommand{
		clientConfig:   clientConfig,
		assetOutputDir: "/tmp/",
		runType:        "task",
	}
	cmd := &cobra.Command{
		Use:     "run-input",
		Short:   "Fetch jobRunInput assets for a scheduled execution",
		Example: "optimus job run-input <job_name> --output-dir </tmp> --scheduled-at <2021-01-14T02:00:00+00:00> --type <task|hook> --name <bq2bq> --project \"project-id\" --namespace \"namespace\" ",
		Args:    cobra.MinimumNArgs(1),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE:    jobRunInput.RunE,
		PreRunE: jobRunInput.PreRunE,
	}
	cmd.Flags().StringVar(&jobRunInput.assetOutputDir, "output-dir", jobRunInput.assetOutputDir, "Output directory for assets")
	cmd.MarkFlagRequired("output-dir")
	cmd.Flags().StringVar(&jobRunInput.scheduledAt, "scheduled-at", "", "Time at which the job was scheduled for execution")
	cmd.MarkFlagRequired("scheduled-at")
	cmd.Flags().StringVar(&jobRunInput.runType, "type", "task", "Type of instance, could be task/hook")
	cmd.MarkFlagRequired("type")
	cmd.Flags().StringVar(&jobRunInput.runName, "name", "", "Name of instancetype")
	cmd.MarkFlagRequired("name")

	cmd.Flags().StringP("project-name", "p", defaultProjectName, "Name of the optimus project")
	cmd.MarkFlagRequired("project-name")
	cmd.Flags().String("host", defaultHost, "Optimus service endpoint url")

	return cmd
}

func (b *jobRunInputCommand) PreRunE(_ *cobra.Command, _ []string) error {
	b.logger = logger.NewClientLogger(b.clientConfig.Log)
	return nil
}

func (b *jobRunInputCommand) RunE(_ *cobra.Command, args []string) error {
	jobName := args[0]
	b.logger.Info(fmt.Sprintf("Requesting resources for project %s, job %s at %s", b.clientConfig.Project.Name, jobName, b.clientConfig.Host))
	b.logger.Info(fmt.Sprintf("Run name %s, run type %s, scheduled at %s\n", b.runName, b.runType, b.scheduledAt))
	b.logger.Info("please wait...")

	jobScheduledTimeProto, err := b.getJobScheduledTimeProto()
	if err != nil {
		return fmt.Errorf("invalid time format, please use %s: %w", models.InstanceScheduledAtTimeLayout, err)
	}

	jobResponse, err := b.sendJobRunInputRequest(jobName, jobScheduledTimeProto)
	if err != nil {
		return fmt.Errorf("request failed for job %s: %w", jobName, err)
	}
	return b.writeInstanceResponse(jobResponse)
}

// writeInstanceResponse fetches a JobRun from the store (eg, postgres)
// Based on the response, it builds assets like query, env and config
// for the Job Run which is saved into output files.
func (b *jobRunInputCommand) writeInstanceResponse(jobResponse *pb.JobRunInputResponse) (err error) {
	dirPath := filepath.Join(b.assetOutputDir, taskInputDirectory)
	if err := b.writeJobAssetsToFiles(jobResponse, dirPath); err != nil {
		return fmt.Errorf("error writing response map to file: %w", err)
	}

	if err := b.writeJobResponseEnvToFile(jobResponse, dirPath); err != nil {
		return fmt.Errorf("error writing response env to file: %w", err)
	}

	if err := b.writeJobResponseSecretToFile(jobResponse, dirPath); err != nil {
		return fmt.Errorf("error writing response env to file: %w", err)
	}

	if len(b.keysWithUnsubstitutedValue) > 0 {
		b.logger.Warn(logger.ColoredNotice("Value not substituted for keys:\n%s", strings.Join(b.keysWithUnsubstitutedValue, "\n")))
	}
	return nil
}

func (b *jobRunInputCommand) writeJobResponseSecretToFile(
	jobResponse *pb.JobRunInputResponse, dirPath string,
) error {
	// write all secrets into a file
	secretsFileContent := ""
	for key, val := range jobResponse.Secrets {
		if strings.Contains(val, unsubstitutedValue) {
			b.keysWithUnsubstitutedValue = append(b.keysWithUnsubstitutedValue, key)
		}
		secretsFileContent += fmt.Sprintf("%s='%s'\n", key, val)
	}

	filePath := filepath.Join(dirPath, models.InstanceDataTypeSecretFileName)
	writeToFileFn := utils.WriteStringToFileIndexed()
	if err := writeToFileFn(filePath, secretsFileContent, b.logger.Writer()); err != nil {
		return fmt.Errorf("failed to write asset file at %s: %w", filePath, err)
	}
	return nil
}

func (b *jobRunInputCommand) writeJobResponseEnvToFile(
	jobResponse *pb.JobRunInputResponse, dirPath string,
) error {
	envFileBlob := ""
	for key, val := range jobResponse.Envs {
		if strings.Contains(val, unsubstitutedValue) {
			b.keysWithUnsubstitutedValue = append(b.keysWithUnsubstitutedValue, key)
		}
		envFileBlob += fmt.Sprintf("%s='%s'\n", key, val)
	}

	filePath := filepath.Join(dirPath, models.InstanceDataTypeEnvFileName)
	writeToFileFn := utils.WriteStringToFileIndexed()
	if err := writeToFileFn(filePath, envFileBlob, b.logger.Writer()); err != nil {
		return fmt.Errorf("failed to write asset file at %s: %w", filePath, err)
	}
	return nil
}

func (b *jobRunInputCommand) writeJobAssetsToFiles(
	jobResponse *pb.JobRunInputResponse, dirPath string,
) error {
	permission := 600
	if err := os.MkdirAll(dirPath, fs.FileMode(permission)); err != nil {
		return fmt.Errorf("failed to create directory at %s: %w", dirPath, err)
	}

	writeToFileFn := utils.WriteStringToFileIndexed()
	for fileName, fileContent := range jobResponse.Files {
		filePath := filepath.Join(dirPath, fileName)
		if err := writeToFileFn(filePath, fileContent, b.logger.Writer()); err != nil {
			return fmt.Errorf("failed to write asset file at %s: %w", filePath, err)
		}
	}
	return nil
}

func (b *jobRunInputCommand) sendJobRunInputRequest(jobName string, jobScheduledTimeProto *timestamppb.Timestamp) (*pb.JobRunInputResponse, error) {
	conn, err := connectivity.NewConnectivity(b.clientConfig.Host, jobRunInputCompileAssetsTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// fetch Instance by calling the optimus API
	jobRunInputService := pb.NewJobRunServiceClient(conn.GetConnection())
	request := &pb.RegisterInstanceRequest{
		ProjectName:  b.clientConfig.Project.Name,
		JobName:      jobName,
		ScheduledAt:  jobScheduledTimeProto,
		InstanceType: pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[utils.ToEnumProto(b.runType, "type")]),
		InstanceName: b.runName,
	}
	return jobRunInputService.GetJobRunInput(conn.GetContext(), request)
}

func (b *jobRunInputCommand) getJobScheduledTimeProto() (*timestamppb.Timestamp, error) {
	jobScheduledTime, err := time.Parse(models.InstanceScheduledAtTimeLayout, b.scheduledAt)
	if err != nil {
		return nil, err
	}
	return timestamppb.New(jobScheduledTime), nil
}
