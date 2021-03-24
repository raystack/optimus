package commands

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/odpf/optimus/config"

	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	v1handler "github.com/odpf/optimus/api/handler/v1"
	pb "github.com/odpf/optimus/api/proto/v1"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

var (
	errRequestFail            = errors.New("🔥 unable to complete request successfully")
	errRequestTimedOut        = errors.New("🔥 request timed out while checking the status")
	errResponseParseError     = errors.New("🔥 unable to parse server response")
	errUnhandledServerRequest = errors.New("🔥 server is unable to process this request at the moment")

	// (kush.sharma)
	// If application ever gets slower than 5 mins, instead of increasing timeout
	// fix where its taking that much time and optimise!
	deploymentTimeout = time.Minute * 5
)

// deployCommand pushes current repo to optimus service
func deployCommand(l logger, jobSpecRepo store.JobSpecRepository, conf config.Opctl) *cli.Command {
	var projectName string
	var ignoreJobs bool

	cmd := &cli.Command{
		Use:   "deploy",
		Short: "Deploy current project to server",
	}
	cmd.Flags().StringVar(&projectName, "project", "", "project name of deployee")
	cmd.MarkFlagRequired("project")
	cmd.Flags().BoolVar(&ignoreJobs, "ignore-jobs", false, "ignore deployment of jobs")

	cmd.Run = func(c *cli.Command, args []string) {
		l.Printf("deploying project %s at %s\nplease wait...\n", projectName, conf.Host)

		if err := postDeploymentRequest(l, projectName, jobSpecRepo, conf, ignoreJobs); err != nil {
			l.Println(err)
			l.Println(errRequestFail)
			os.Exit(1)
		}
	}

	return cmd
}

// postDeploymentRequest send a deployment request to service
func postDeploymentRequest(l logger, projectName string, jobSpecRepo store.JobSpecRepository, conf config.Opctl, ignoreJobDeployment bool) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, conf.Host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Println("can't reach optimus service")
		}
		return err
	}
	defer conn.Close()

	deployTimeoutCtx, deployCancel := context.WithTimeout(context.Background(), deploymentTimeout)
	defer deployCancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	adapt := v1handler.NewAdapter(models.TaskRegistry, models.HookRegistry)

	// update project config if needed
	registerResponse, err := runtime.RegisterProject(deployTimeoutCtx, &pb.RegisterProjectRequest{
		Project: &pb.ProjectSpecification{
			Name:   projectName,
			Config: conf.Global,
		},
	})
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to update project configurations, %s", registerResponse.Message))
	} else if !registerResponse.Success {
		return fmt.Errorf("failed to update project configurations, %s", registerResponse.Message)
	}
	l.Println("updated project configuration")

	if ignoreJobDeployment {
		// skip job deployment if this is true
		l.Println("skipping job deployment")
		return nil
	}

	// deploy job specifications
	l.Println("deploying jobs")
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
	respStream, err := runtime.DeploySpecification(deployTimeoutCtx, &pb.DeploySpecificationRequest{
		Jobs:        adaptedJobSpecs,
		ProjectName: projectName,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Println("deployment process took too long, timing out")
		}
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
