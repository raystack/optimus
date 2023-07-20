package scheduler

import (
	"context"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/raystack/salt/log"
	"github.com/spf13/cobra"

	"github.com/raystack/optimus/client/cmd/internal/connection"
	"github.com/raystack/optimus/client/cmd/internal/logger"
	"github.com/raystack/optimus/config"
	pb "github.com/raystack/optimus/protos/raystack/optimus/core/v1beta1"
)

const (
	uploadTimeout = time.Minute * 30
)

type uploadCommand struct {
	logger     log.Logger
	connection connection.Connection

	clientConfig *config.ClientConfig

	configFilePath string
}

// UploadCommand initializes command for scheduler DAG deployment
func UploadCommand() *cobra.Command {
	upload := &uploadCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "upload-all",
		Short:   "Deploy current optimus project to server",
		Long:    heredoc.Doc(`Apply changes to destination server which includes creating/updating/deleting jobs`),
		Example: "optimus scheduler upload-all",
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE:    upload.RunE,
		PreRunE: upload.PreRunE,
	}
	cmd.Flags().StringVarP(&upload.configFilePath, "config", "c", upload.configFilePath, "File path for client configuration")
	return cmd
}

func (u *uploadCommand) PreRunE(_ *cobra.Command, _ []string) error {
	var err error
	u.clientConfig, err = config.LoadClientConfig(u.configFilePath)
	if err != nil {
		return err
	}

	u.connection = connection.New(u.logger, u.clientConfig)
	u.logger.Info("initialization finished!\n")
	return err
}

func (u *uploadCommand) RunE(_ *cobra.Command, _ []string) error {
	u.logger.Info("Uploading jobs for project " + u.clientConfig.Project.Name)

	_, err := u.sendUploadAllRequest(u.clientConfig.Project.Name)
	if err != nil {
		u.logger.Error("Error: %v", err.Error())
		return err
	}
	u.logger.Info("Triggered upload to scheduler, changes will be reflected in scheduler after a few minutes")
	return nil
}

func (u *uploadCommand) sendUploadAllRequest(projectName string) (*pb.UploadToSchedulerResponse, error) {
	conn, err := u.connection.Create(u.clientConfig.Host)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	request := &pb.UploadToSchedulerRequest{
		ProjectName: projectName,
	}
	jobRunServiceClient := pb.NewJobRunServiceClient(conn)

	ctx, cancelFunc := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancelFunc()

	return jobRunServiceClient.UploadToScheduler(ctx, request)
}
