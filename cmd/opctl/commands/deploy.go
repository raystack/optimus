package commands

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	v1handler "github.com/odpf/optimus/api/handler/v1"
	pb "github.com/odpf/optimus/api/proto/v1"
	"github.com/odpf/optimus/store"
)

var (
	deployHost                string
	deployCommitID            string
	deployProject             string
	errDeployFail             = errors.New("🔥 unable to complete request successfully")
	errRequestTimedOut        = errors.New("🔥 request timed out while checking the status")
	errResponseParseError     = errors.New("🔥 unable to parse server response")
	errUnhandledServerRequest = errors.New("🔥 server is unable to process this request at the moment")

	ServerPort = 11000
)

// deployCommand pushes current repo to optimus service
func deployCommand(l logger, jobSpecRepo store.JobSpecRepository) *cli.Command {

	cmd := &cli.Command{
		Use:   "deploy",
		Short: "Deploy current project to server",
	}
	cmd.Flags().StringVar(&deployHost, "host", "", "deployment service endpoint url")
	cmd.MarkFlagRequired("host")
	cmd.Flags().StringVar(&deployCommitID, "commit", "", "commit hash of current repo")
	cmd.MarkFlagRequired("commit")
	cmd.Flags().StringVar(&deployProject, "project", "", "project name of deployee")
	cmd.MarkFlagRequired("project")

	cmd.Run = func(c *cli.Command, args []string) {
		l.Printf("deploying commit %s to %s\nplease wait...\n", deployCommitID, deployHost)

		if err := postDeploymentRequest(l, jobSpecRepo); err != nil {
			l.Print(err)
			l.Print(errDeployFail)
			os.Exit(1)
		}
	}

	return cmd
}

// postDeploymentRequest send a deployment request to service
func postDeploymentRequest(l logger, jobSpecRepo store.JobSpecRepository) (err error) {
	var conn *grpc.ClientConn
	if conn, err = createConnection(ServerPort); err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	adapt := v1handler.NewAdapter()

	jobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return err
	}

	for _, spec := range jobSpecs {
		resp, err := runtime.DeploySpecification(ctx, &pb.DeploySpecificationRequest{
			Job: adapt.ToJobProto(spec),
		})
		if err != nil {
			return errors.Wrapf(err, "failed during processing: %s", spec.Name)
		}
		if !resp.Succcess {
			return errors.Errorf("unable to deploy: %s, %s", spec.Name, resp.Error)
		}
	}

	l.Printf("deployment completed successfully")
	return nil
}

func createConnection(port int) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())

	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", port), opts...)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
