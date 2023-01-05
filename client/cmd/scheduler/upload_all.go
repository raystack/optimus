package scheduler

import (
	"fmt"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/config"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

const (
	uploadTimeout = time.Minute * 30
)

type uploadCommand struct {
	logger       log.Logger
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

	u.logger.Info("initialization finished!\n")
	return err
}

func (u *uploadCommand) RunE(_ *cobra.Command, _ []string) error {
	u.logger.Info("Uploading jobs for project " + u.clientConfig.Project.Name)
	u.logger.Info("please wait...")

	uploadResponse, err := u.sendUploadAllRequest(u.clientConfig.Project.Name)
	if err != nil {
		return fmt.Errorf("request failed for project %s: %w, %s", u.clientConfig.Project.Name, err, uploadResponse.ErrorMessage)
	}
	if uploadResponse.Status {
		u.logger.Info("Uploaded jobs for project " + u.clientConfig.Project.Name)
	} else {
		u.logger.Error("Error Uploading jobs for project: "+u.clientConfig.Project.Name+", error: ", uploadResponse.ErrorMessage)
	}
	return nil
}

func (u *uploadCommand) sendUploadAllRequest(projectName string) (*pb.UploadToSchedulerResponse, error) {
	conn, err := connectivity.NewConnectivity(u.clientConfig.Host, uploadTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	request := &pb.UploadToSchedulerRequest{
		ProjectName: projectName,
	}
	jobRunServiceClient := pb.NewJobRunServiceClient(conn.GetConnection())
	return jobRunServiceClient.UploadToScheduler(conn.GetContext(), request)
}
