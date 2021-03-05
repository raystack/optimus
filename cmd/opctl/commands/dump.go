package commands

import (
	"context"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	"os"
	pb "github.com/odpf/optimus/api/proto/v1"
)

func dumpCommand(l logger) *cli.Command {
	cmd := &cli.Command{
		Use:     "dump",
		Short:   "write the representation of the resource to stdout",
		Args:    cli.MinimumNArgs(1),
		Example: "opctl dump <job_name> --host localhost:6666 --project g-pilotdata-gl",
	}

	cmd.Flags().StringVar(&optimusHost, "host", "", "optimus service endpoint url")
	cmd.MarkFlagRequired("host")
	cmd.Flags().StringVar(&projectName, "project", "", "name of the tenant")
	cmd.MarkFlagRequired("project")

	cmd.Run = func(c *cli.Command, args []string) {
		jobName := args[0]
		if err := dumpSpecificationBuildRequest(l, projectName, jobName); err != nil {
			l.Println(err)
			l.Println(errRequestFail)
			os.Exit(1)
		}
	}

	return cmd
}

func dumpSpecificationBuildRequest(l logger, projectName, jobName string) (err error) {
	var conn *grpc.ClientConn
	if conn, err = createConnection(optimusHost); err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	// fetch compiled JobSpec by calling the optimus API
	jobResponse, err := runtime.DumpSpecification(ctx, &pb.DumpSpecificationRequest{
		ProjectName: projectName,
		JobName:     jobName,
	})
	if err != nil {
		return errors.Wrapf(err, "request failed for job %s", jobName)
	}

	l.Println(jobResponse.GetContent())
	return nil
}
