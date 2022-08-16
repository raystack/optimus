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
	"github.com/odpf/optimus/cmd/internal"
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
	logger         log.Logger
	configFilePath string

	assetOutputDir string
	runType        string
	runName        string
	scheduledAt    string
	projectName    string
	host           string

	keysWithUnsubstitutedValue []string
}

// NewJobRunInputCommand gets compiled assets required for a job run
func NewJobRunInputCommand() *cobra.Command {
	jobRunInput := &jobRunInputCommand{
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
	jobRunInput.injectFlags(cmd)
	internal.MarkFlagsRequired(cmd, []string{"output-dir", "scheduled-at", "type", "name"})

	return cmd
}

func (j *jobRunInputCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&j.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	// Mandatory flags
	cmd.Flags().StringVar(&j.assetOutputDir, "output-dir", j.assetOutputDir, "Output directory for assets")
	cmd.Flags().StringVar(&j.scheduledAt, "scheduled-at", "", "Time at which the job was scheduled for execution")
	cmd.Flags().StringVar(&j.runType, "type", "task", "Type of instance, could be task/hook")
	cmd.Flags().StringVar(&j.runName, "name", "", "Name of running instance, e.g., bq2bq/transporter/predator")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&j.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&j.host, "host", "", "Optimus service endpoint url")
}

func (j *jobRunInputCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(j.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		j.logger = logger.NewDefaultLogger()
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	j.logger = logger.NewClientLogger(conf.Log)
	if j.projectName == "" {
		j.projectName = conf.Project.Name
	}
	if j.host == "" {
		j.host = conf.Host
	}

	return nil
}

func (j *jobRunInputCommand) RunE(_ *cobra.Command, args []string) error {
	jobName := args[0]
	j.logger.Info(fmt.Sprintf("Requesting resources for project %s, job %s at %s", j.projectName, jobName, j.host))
	j.logger.Info(fmt.Sprintf("Run name %s, run type %s, scheduled at %s\n", j.runName, j.runType, j.scheduledAt))
	j.logger.Info("please wait...")

	jobScheduledTimeProto, err := j.getJobScheduledTimeProto()
	if err != nil {
		return fmt.Errorf("invalid time format, please use %s: %w", models.InstanceScheduledAtTimeLayout, err)
	}

	jobResponse, err := j.sendJobRunInputRequest(jobName, jobScheduledTimeProto)
	if err != nil {
		return fmt.Errorf("request failed for job %s: %w", jobName, err)
	}
	return j.writeInstanceResponse(jobResponse)
}

// writeInstanceResponse fetches a JobRun from the store (eg, postgres)
// Based on the response, it builds assets like query, env and config
// for the Job Run which is saved into output files.
func (j *jobRunInputCommand) writeInstanceResponse(jobResponse *pb.JobRunInputResponse) (err error) {
	dirPath := filepath.Join(j.assetOutputDir, taskInputDirectory)
	if err := j.writeJobAssetsToFiles(jobResponse, dirPath); err != nil {
		return fmt.Errorf("error writing response map to file: %w", err)
	}

	if err := j.writeJobResponseEnvToFile(jobResponse, dirPath); err != nil {
		return fmt.Errorf("error writing response env to file: %w", err)
	}

	if err := j.writeJobResponseSecretToFile(jobResponse, dirPath); err != nil {
		return fmt.Errorf("error writing response env to file: %w", err)
	}

	if len(j.keysWithUnsubstitutedValue) > 0 {
		j.logger.Warn(logger.ColoredNotice("Value not substituted for keys:\n%s", strings.Join(j.keysWithUnsubstitutedValue, "\n")))
	}
	return nil
}

func (j *jobRunInputCommand) writeJobResponseSecretToFile(
	jobResponse *pb.JobRunInputResponse, dirPath string,
) error {
	// write all secrets into a file
	secretsFileContent := ""
	for key, val := range jobResponse.Secrets {
		if strings.Contains(val, unsubstitutedValue) {
			j.keysWithUnsubstitutedValue = append(j.keysWithUnsubstitutedValue, key)
		}
		secretsFileContent += fmt.Sprintf("%s='%s'\n", key, val)
	}

	filePath := filepath.Join(dirPath, models.InstanceDataTypeSecretFileName)
	writeToFileFn := utils.WriteStringToFileIndexed()
	if err := writeToFileFn(filePath, secretsFileContent, j.logger.Writer()); err != nil {
		return fmt.Errorf("failed to write asset file at %s: %w", filePath, err)
	}
	return nil
}

func (j *jobRunInputCommand) writeJobResponseEnvToFile(jobResponse *pb.JobRunInputResponse, dirPath string) error {
	envFileBlob := ""
	for key, val := range jobResponse.Envs {
		if strings.Contains(val, unsubstitutedValue) {
			j.keysWithUnsubstitutedValue = append(j.keysWithUnsubstitutedValue, key)
		}
		envFileBlob += fmt.Sprintf("%s='%s'\n", key, val)
	}

	filePath := filepath.Join(dirPath, models.InstanceDataTypeEnvFileName)
	writeToFileFn := utils.WriteStringToFileIndexed()
	if err := writeToFileFn(filePath, envFileBlob, j.logger.Writer()); err != nil {
		return fmt.Errorf("failed to write asset file at %s: %w", filePath, err)
	}
	return nil
}

func (j *jobRunInputCommand) writeJobAssetsToFiles(
	jobResponse *pb.JobRunInputResponse, dirPath string,
) error {
	permission := 600
	if err := os.MkdirAll(dirPath, fs.FileMode(permission)); err != nil {
		return fmt.Errorf("failed to create directory at %s: %w", dirPath, err)
	}

	writeToFileFn := utils.WriteStringToFileIndexed()
	for fileName, fileContent := range jobResponse.Files {
		filePath := filepath.Join(dirPath, fileName)
		if err := writeToFileFn(filePath, fileContent, j.logger.Writer()); err != nil {
			return fmt.Errorf("failed to write asset file at %s: %w", filePath, err)
		}
	}
	return nil
}

func (j *jobRunInputCommand) sendJobRunInputRequest(jobName string, jobScheduledTimeProto *timestamppb.Timestamp) (*pb.JobRunInputResponse, error) {
	conn, err := connectivity.NewConnectivity(j.host, jobRunInputCompileAssetsTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// fetch Instance by calling the optimus API
	jobRunServiceClient := pb.NewJobRunServiceClient(conn.GetConnection())
	request := &pb.JobRunInputRequest{
		ProjectName:  j.projectName,
		JobName:      jobName,
		ScheduledAt:  jobScheduledTimeProto,
		InstanceType: pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[utils.ToEnumProto(j.runType, "type")]),
		InstanceName: j.runName,
	}

	return jobRunServiceClient.JobRunInput(conn.GetContext(), request)
}

func (j *jobRunInputCommand) getJobScheduledTimeProto() (*timestamppb.Timestamp, error) {
	jobScheduledTime, err := time.Parse(models.InstanceScheduledAtTimeLayout, j.scheduledAt)
	if err != nil {
		return nil, err
	}
	return timestamppb.New(jobScheduledTime), nil
}
