package commands

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	v1handler "github.com/odpf/optimus/api/handler/v1"
	pb "github.com/odpf/optimus/api/proto/v1"
	"github.com/odpf/optimus/models"
)

var (
	projectName    string
	assetOutputDir string
)

// getCommand requests a resource from optimus
func getCommand(l logger) *cli.Command {
	cmd := &cli.Command{
		Use:   "get",
		Short: "Get a resource",
	}
	cmd.AddCommand(getJobSubCommand(l))
	return cmd
}

func getJobSubCommand(l logger) *cli.Command {

	cmd := &cli.Command{
		Use:     "job",
		Short:   "Get a Job including the assets",
		Example: "opctl get job sample_replace --project \"project-id\"",
		Args:    cli.MinimumNArgs(1),
	}
	cmd.Flags().StringVar(&projectName, "project", "", "project name of deployee")
	cmd.MarkFlagRequired("project")
	cmd.Flags().StringVar(&assetOutputDir, "output-dir", "", "output directory for assets")
	cmd.MarkFlagRequired("output-dir")
	cmd.Flags().StringVar(&deployHost, "host", "", "deployment service endpoint url")
	cmd.MarkFlagRequired("host")

	cmd.Run = func(c *cli.Command, args []string) {
		jobName := args[0]
		l.Printf("requesting resources for project %s, job %s at %s\nplease wait...\n", deployProject, jobName, deployHost)

		if err := getJobRequest(l, jobName); err != nil {
			l.Print(err)
			l.Print(errDeployFail)
			os.Exit(1)
		}
	}
	return cmd
}

// getJobRequest send a job request to service
func getJobRequest(l logger, jobName string) (err error) {
	var conn *grpc.ClientConn
	if conn, err = createConnection(deployHost); err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	adapt := v1handler.NewAdapter(models.TaskRegistry)

	jobResponse, err := runtime.GetJob(ctx, &pb.GetJobRequest{
		ProjectName: projectName,
		JobName:     jobName,
	})
	if err != nil {
		return errors.Wrapf(err, "request failed for job %s", jobName)
	}

	jobSpec, err := adapt.FromJobProto(jobResponse.GetJob())
	if err != nil {
		return errors.Wrapf(err, "failed to parse job %s", jobName)
	}

	for idx, jobAsset := range jobSpec.Assets.GetAll() {
		filePath := filepath.Join(assetOutputDir, jobAsset.Name)
		if err := ioutil.WriteFile(filePath,
			[]byte(jobAsset.Value), 0644); err != nil {
			return errors.Wrapf(err, "failed to write asset file at %s", filePath)
		}
		l.Printf("%d. writing asset at %s\n", idx+1, filePath)
	}

	return nil
}
