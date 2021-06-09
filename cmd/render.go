package cmd

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/odpf/optimus/instance"
	"github.com/odpf/optimus/models"

	"github.com/odpf/optimus/utils"

	"github.com/odpf/optimus/store"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	renderTimeout  = time.Minute * 2
	templateEngine = instance.NewGoEngine()
)

func renderCommand(l logger, host string, jobSpecRepo store.JobSpecRepository) *cli.Command {
	cmd := &cli.Command{
		Use:   "render",
		Short: "convert raw representation of specification to consumables",
	}
	if jobSpecRepo != nil {
		cmd.AddCommand(renderTemplateCommand(l, jobSpecRepo))
	}
	cmd.AddCommand(renderJobCommand(l, host))
	return cmd
}

func renderTemplateCommand(l logger, jobSpecRepo store.JobSpecRepository) *cli.Command {
	cmd := &cli.Command{
		Use:     "template",
		Short:   "render templates for a job to current 'render' directory",
		Example: "optimus render template",
	}

	cmd.RunE = func(c *cli.Command, args []string) error {
		var err error
		var jobName string
		if len(args) == 0 {
			// doing it locally for now, ideally using optimus service will give
			// more accurate results
			jobName, err = selectJobSurvey(jobSpecRepo)
			if err != nil {
				return err
			}
		} else {
			jobName = args[0]
		}
		jobSpec, _ := jobSpecRepo.GetByName(jobName)

		// create temporary directory
		renderedPath := filepath.Join(".", "render", jobSpec.Name)
		_ = os.MkdirAll(renderedPath, 0770)
		l.Println("rendering assets in", renderedPath)

		now := time.Now()
		l.Println("assuming execution time as current time of", now.Format(models.InstanceScheduledAtTimeLayout))

		templates, err := instance.DumpAssets(jobSpec, now, templateEngine, true)
		if err != nil {
			return err
		}

		writeToFileFn := utils.WriteStringToFileIndexed()
		for name, content := range templates {
			if err := writeToFileFn(filepath.Join(renderedPath, name), content, l.Writer()); err != nil {
				return err
			}
		}

		l.Println(coloredSuccess("render complete"))
		return nil
	}

	return cmd
}

func renderJobCommand(l logger, host string) *cli.Command {
	var projectName string
	var namespace string
	cmd := &cli.Command{
		Use:     "job",
		Short:   "write the scheduler representation of the job to stdout",
		Args:    cli.MinimumNArgs(1),
		Example: "optimus render job <job_name> --project g-optimus",
	}
	cmd.Flags().StringVar(&projectName, "project", "", "name of the project")
	cmd.MarkFlagRequired("project")
	cmd.Flags().StringVar(&namespace, "namespace", "", "namespace")
	cmd.MarkFlagRequired("namespace")

	cmd.RunE = func(c *cli.Command, args []string) error {
		jobName := args[0]
		return renderJobSpecificationBuildRequest(l, projectName, namespace, jobName, host)
	}

	return cmd
}

func renderJobSpecificationBuildRequest(l logger, projectName, namespace, jobName string, host string) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Println(coloredError("can't reach optimus service"))
		}
		return err
	}
	defer conn.Close()

	dumpTimeoutCtx, dumpCancel := context.WithTimeout(context.Background(), renderTimeout)
	defer dumpCancel()

	l.Println("please wait...")
	runtime := pb.NewRuntimeServiceClient(conn)
	// fetch compiled JobSpec by calling the optimus API
	jobResponse, err := runtime.DumpJobSpecification(dumpTimeoutCtx, &pb.DumpJobSpecificationRequest{
		ProjectName: projectName,
		JobName:     jobName,
		Namespace:   namespace,
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Println("render process took too long, timing out")
		}
		return errors.Wrapf(err, "request failed for job %s", jobName)
	}

	l.Println(jobResponse.GetContent())
	return nil
}
