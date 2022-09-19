package admin

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
	"github.com/odpf/optimus/cmd/internal"
	"github.com/odpf/optimus/cmd/internal/connectivity"
	"github.com/odpf/optimus/cmd/internal/logger"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
)

const (
	adminBuildInstanceTimeout = time.Minute * 1

	taskInputDirectory = "in"
	unsubstitutedValue = "<no value>"
)

type buildInstanceCommand struct {
	logger         log.Logger
	configFilePath string

	// Required
	assetOutputDir string
	runType        string
	runName        string
	scheduledAt    string
	projectName    string
	host           string

	keysWithUnsubstitutedValue []string
}

// NewBuildInstanceCommand initializes command to build instance for admin
func NewBuildInstanceCommand() *cobra.Command {
	buildInstance := &buildInstanceCommand{
		assetOutputDir: "/tmp/",
		runType:        "task",
		logger:         logger.NewClientLogger(),
	}
	cmd := &cobra.Command{
		Use:     "instance",
		Short:   "Builds a Job instance including the assets for a scheduled execution",
		Example: "optimus admin build instance <sample_replace> --output-dir </tmp> --scheduled-at <2021-01-14T02:00:00+00:00> --type task --name <bq2bq> [--project \"project-id\"]",
		Args:    cobra.MinimumNArgs(1),
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE:    buildInstance.RunE,
		PreRunE: buildInstance.PreRunE,
	}

	buildInstance.injectFlags(cmd)
	internal.MarkFlagsRequired(cmd, []string{"output-dir", "scheduled-at", "type", "name"})

	return cmd
}

func (b *buildInstanceCommand) injectFlags(cmd *cobra.Command) {
	// Config filepath flag
	cmd.Flags().StringVarP(&b.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	// Mandatory flags
	cmd.Flags().StringVar(&b.assetOutputDir, "output-dir", b.assetOutputDir, "Output directory for assets")
	cmd.Flags().StringVar(&b.scheduledAt, "scheduled-at", "", "Time at which the job was scheduled for execution")
	cmd.Flags().StringVar(&b.runType, "type", "task", "Type of instance, could be task/hook")
	cmd.Flags().StringVar(&b.runName, "name", "", "Name of running instance, e.g., bq2bq/transporter/predator")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&b.projectName, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&b.host, "host", "", "Optimus service endpoint url")
}

func (b *buildInstanceCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	// Load config
	conf, err := internal.LoadOptionalConfig(b.configFilePath)
	if err != nil {
		return err
	}

	if conf == nil {
		internal.MarkFlagsRequired(cmd, []string{"project-name", "host"})
		return nil
	}

	if b.projectName == "" {
		b.projectName = conf.Project.Name
	}
	if b.host == "" {
		b.host = conf.Host
	}
	return nil
}

func (b *buildInstanceCommand) RunE(_ *cobra.Command, args []string) error {
	jobName := args[0]
	b.logger.Info("Requesting resources for project %s, job %s at %s", b.projectName, jobName, b.host)
	b.logger.Info("Run name %s, run type %s, scheduled at %s", b.runName, b.runType, b.scheduledAt)
	b.logger.Info("please wait...")

	jobScheduledTimeProto, err := b.getJobScheduledTimeProto()
	if err != nil {
		return fmt.Errorf("invalid time format, please use %s: %w", models.InstanceScheduledAtTimeLayout, err)
	}

	jobResponse, err := b.sendInstanceRequest(jobName, jobScheduledTimeProto)
	if err != nil {
		return fmt.Errorf("request failed for job %s: %w", jobName, err)
	}
	return b.writeInstanceResponse(jobResponse)
}

// writeInstanceResponse fetches a JobRun from the store (eg, postgres)
// Based on the response, it builds assets like query, env and config
// for the Job Run which is saved into output files.
func (b *buildInstanceCommand) writeInstanceResponse(jobResponse *pb.RegisterInstanceResponse) (err error) {
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
		b.logger.Warn("Value not substituted for keys:\n%s", strings.Join(b.keysWithUnsubstitutedValue, "\n"))
	}
	return nil
}

func (b *buildInstanceCommand) writeJobResponseSecretToFile(
	jobResponse *pb.RegisterInstanceResponse, dirPath string,
) error {
	// write all secrets into a file
	secretsFileContent := ""
	for key, val := range jobResponse.Context.Secrets {
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

func (b *buildInstanceCommand) writeJobResponseEnvToFile(
	jobResponse *pb.RegisterInstanceResponse, dirPath string,
) error {
	envFileBlob := ""
	for key, val := range jobResponse.Context.Envs {
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

func (b *buildInstanceCommand) writeJobAssetsToFiles(
	jobResponse *pb.RegisterInstanceResponse, dirPath string,
) error {
	permission := 600
	if err := os.MkdirAll(dirPath, fs.FileMode(permission)); err != nil {
		return fmt.Errorf("failed to create directory at %s: %w", dirPath, err)
	}

	writeToFileFn := utils.WriteStringToFileIndexed()
	for fileName, fileContent := range jobResponse.Context.Files {
		filePath := filepath.Join(dirPath, fileName)
		if err := writeToFileFn(filePath, fileContent, b.logger.Writer()); err != nil {
			return fmt.Errorf("failed to write asset file at %s: %w", filePath, err)
		}
	}
	return nil
}

func (b *buildInstanceCommand) sendInstanceRequest(jobName string, jobScheduledTimeProto *timestamppb.Timestamp) (*pb.RegisterInstanceResponse, error) {
	conn, err := connectivity.NewConnectivity(b.host, adminBuildInstanceTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// fetch Instance by calling the optimus API
	jobRun := pb.NewJobRunServiceClient(conn.GetConnection())
	request := &pb.RegisterInstanceRequest{
		ProjectName:  b.projectName,
		JobName:      jobName,
		ScheduledAt:  jobScheduledTimeProto,
		InstanceType: pb.InstanceSpec_Type(pb.InstanceSpec_Type_value[utils.ToEnumProto(b.runType, "type")]),
		InstanceName: b.runName,
	}
	return jobRun.RegisterInstance(conn.GetContext(), request)
}

func (b *buildInstanceCommand) getJobScheduledTimeProto() (*timestamppb.Timestamp, error) {
	jobScheduledTime, err := time.Parse(models.InstanceScheduledAtTimeLayout, b.scheduledAt)
	if err != nil {
		return nil, err
	}
	return timestamppb.New(jobScheduledTime), nil
}
