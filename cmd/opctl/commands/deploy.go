package commands

import (
	"context"
	"io"
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
	optimusHost               string
	deployProject             string
	errRequestFail            = errors.New("🔥 unable to complete request successfully")
	errRequestTimedOut        = errors.New("🔥 request timed out while checking the status")
	errResponseParseError     = errors.New("🔥 unable to parse server response")
	errUnhandledServerRequest = errors.New("🔥 server is unable to process this request at the moment")
)

// deployCommand pushes current repo to optimus service
func deployCommand(l logger, jobSpecRepo store.JobSpecRepository) *cli.Command {

	cmd := &cli.Command{
		Use:   "deploy",
		Short: "Deploy current project to server",
	}
	cmd.Flags().StringVar(&optimusHost, "host", "", "deployment service endpoint url")
	cmd.MarkFlagRequired("host")
	cmd.Flags().StringVar(&deployProject, "project", "", "project name of deployee")
	cmd.MarkFlagRequired("project")

	cmd.Run = func(c *cli.Command, args []string) {
		l.Printf("deploying project %s at %s\nplease wait...\n", deployProject, optimusHost)

		if err := postDeploymentRequest(l, jobSpecRepo); err != nil {
			l.Print(err)
			l.Print(errRequestFail)
			os.Exit(1)
		}
	}

	return cmd
}

// postDeploymentRequest send a deployment request to service
func postDeploymentRequest(l logger, jobSpecRepo store.JobSpecRepository) (err error) {
	var conn *grpc.ClientConn
	if conn, err = createConnection(optimusHost); err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	adapt := v1handler.NewAdapter(models.TaskRegistry, models.HookRegistry)

	jobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return err
	}

	adaptedJobSpecs := []*pb.JobSpecification{}
	for _, spec := range jobSpecs {
		adaptJob, err := adapt.ToJobProto(spec)
		if err != nil {
			return errors.Wrapf(err, "failed to serialize: %s", spec.Name)
		}
		adaptedJobSpecs = append(adaptedJobSpecs, adaptJob)
	}
	respStream, err := runtime.DeploySpecification(ctx, &pb.DeploySpecificationRequest{
		Jobs:        adaptedJobSpecs,
		ProjectName: deployProject,
	})
	if err != nil {
		return errors.Wrapf(err, "deployement failed")
	}

	jobCounter := 0
	totalJobs := len(jobSpecs)
	for {
		resp, err := respStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrapf(err, "failed to receive deployment ack")
		}
		if resp.Ack {
			// ack for the job spec
			if !resp.GetSuccess() {
				return errors.Errorf("unable to deploy: %s %s", resp.GetJobName(), resp.GetMessage())
			}
			jobCounter++
			l.Printf("%d/%d. %s successfully deployed\n", jobCounter, totalJobs, resp.GetJobName())
		} else {
			// ordinary progress event
			l.Printf("info '%s': %s\n", resp.GetJobName(), resp.GetMessage())
		}
	}

	l.Println("deployment completed successfully")
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
