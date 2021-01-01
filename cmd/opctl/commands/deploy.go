package commands

import (
	"context"
	"os"

	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	v1handler "github.com/odpf/optimus/api/handler/v1"
	pb "github.com/odpf/optimus/api/proto/v1"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

var (
	deployHost                string
	deployProject             string
	errDeployFail             = errors.New("🔥 unable to complete request successfully")
	errRequestTimedOut        = errors.New("🔥 request timed out while checking the status")
	errResponseParseError     = errors.New("🔥 unable to parse server response")
	errUnhandledServerRequest = errors.New("🔥 server is unable to process this request at the moment")
)

// deployCommand pushes current repo to optimus service
func deployCommand(l logger, jobSpecRepo store.JobRepository) *cli.Command {

	cmd := &cli.Command{
		Use:   "deploy",
		Short: "Deploy current project to server",
	}
	cmd.Flags().StringVar(&deployHost, "host", "", "deployment service endpoint url")
	cmd.MarkFlagRequired("host")
	cmd.Flags().StringVar(&deployProject, "project", "", "project name of deployee")
	cmd.MarkFlagRequired("project")

	cmd.Run = func(c *cli.Command, args []string) {
		l.Printf("deploying project %s at %s\nplease wait...\n", deployProject, deployHost)

		if err := postDeploymentRequest(l, jobSpecRepo); err != nil {
			l.Print(err)
			l.Print(errDeployFail)
			os.Exit(1)
		}
	}

	return cmd
}

// postDeploymentRequest send a deployment request to service
func postDeploymentRequest(l logger, jobSpecRepo store.JobRepository) (err error) {
	var conn *grpc.ClientConn
	if conn, err = createConnection(deployHost); err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	adapt := v1handler.NewAdapter()

	jobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return err
	}

	for idx, spec := range jobSpecs {
		resp, err := runtime.DeploySpecification(ctx, &pb.DeploySpecificationRequest{
			Job: adapt.ToJobProto(spec),
			Project: adapt.ToProjectProto(models.ProjectSpec{
				Name: deployProject,
			}),
		})
		if err != nil {
			return errors.Wrapf(err, "failed during processing: %s", spec.Name)
		}
		if !resp.Succcess {
			return errors.Errorf("unable to deploy: %s, %s", spec.Name, resp.Message)
		}
		l.Printf("%d: %s (deployed)\n", idx+1, spec.Name)
	}

	l.Printf("deployment completed successfully\n")
	return nil
}

func createConnection(host string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())

	conn, err := grpc.Dial(host, opts...)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
