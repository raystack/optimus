package commands

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/odpf/optimus/instance"
	"github.com/odpf/optimus/models"

	"github.com/odpf/optimus/utils"

	"github.com/odpf/optimus/store"

	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	pb "github.com/odpf/optimus/api/proto/v1"
	"github.com/odpf/optimus/config"
)

const (
	renderTimeout = time.Second * 5
)

func renderCommand(l logger, conf config.Opctl, jobSpecRepo store.JobSpecRepository) *cli.Command {
	cmd := &cli.Command{
		Use:   "render",
		Short: "convert raw representation of specification to consumables",
	}
	cmd.AddCommand(renderTemplateCommand(l, conf, jobSpecRepo))
	cmd.AddCommand(renderJobCommand(l, conf))
	return cmd
}

func renderTemplateCommand(l logger, conf config.Opctl, jobSpecRepo store.JobSpecRepository) *cli.Command {
	cmd := &cli.Command{
		Use:     "template",
		Short:   "render templates for a job to current 'render' directory",
		Example: "opctl render template",
	}

	cmd.Run = func(c *cli.Command, args []string) {
		var err error
		jobName := ""
		if len(args) == 0 {
			// doing it locally for now, ideally using optimus service will give
			// more accurate results
			jobName, err = selectJobSurvey(jobSpecRepo)
			if err != nil {
				errExit(l, err)
			}
		} else {
			jobName = args[0]
		}
		jobSpec, _ := jobSpecRepo.GetByName(jobName)

		// create temporary directory
		renderedPath := filepath.Join(".", "render", jobSpec.Name)
		os.MkdirAll(renderedPath, 0770)
		l.Println("rendering assets in", renderedPath)

		now := time.Now()
		l.Println("assuming execution time as current time of", now.Format(models.InstanceScheduledAtTimeLayout))
		jobDestination, err := jobSpec.Task.Unit.GenerateDestination(models.UnitData{
			Config: jobSpec.Task.Config,
			Assets: jobSpec.Assets.ToMap(),
		})
		if err != nil {
			errExit(l, err)
		}

		templates, err := templateEngine.CompileFiles(jobSpec.Assets.ToMap(), map[string]interface{}{
			instance.ConfigKeyDstart:        jobSpec.Task.Window.GetStart(now).Format(models.InstanceScheduledAtTimeLayout),
			instance.ConfigKeyDend:          jobSpec.Task.Window.GetEnd(now).Format(models.InstanceScheduledAtTimeLayout),
			instance.ConfigKeyExecutionTime: now.Format(models.InstanceScheduledAtTimeLayout),
			instance.ConfigKeyDestination:   jobDestination,
		})
		if err != nil {
			errExit(l, err)
		}

		writeToFileFn := utils.WriteStringToFileIndexed()
		for name, content := range templates {
			if err := writeToFileFn(filepath.Join(renderedPath, name), content, l.Writer()); err != nil {
				errExit(l, err)
			}
		}

		l.Println("render complete.")
	}

	return cmd
}

func renderJobCommand(l logger, conf config.Opctl) *cli.Command {
	var projectName string
	cmd := &cli.Command{
		Use:     "job",
		Short:   "write the scheduler representation of the job to stdout",
		Args:    cli.MinimumNArgs(1),
		Example: "opctl render job <job_name> --project g-optimus",
	}
	cmd.Flags().StringVar(&projectName, "project", "", "name of the project")
	cmd.MarkFlagRequired("project")

	cmd.Run = func(c *cli.Command, args []string) {
		jobName := args[0]
		if err := dumpJobSpecificationBuildRequest(l, projectName, jobName, conf); err != nil {
			l.Println(err)
			os.Exit(1)
		}
	}

	return cmd
}

func dumpJobSpecificationBuildRequest(l logger, projectName, jobName string, conf config.Opctl) (err error) {
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

	dumpTimeoutCtx, dumpCancel := context.WithTimeout(context.Background(), renderTimeout)
	defer dumpCancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	// fetch compiled JobSpec by calling the optimus API
	jobResponse, err := runtime.DumpSpecification(dumpTimeoutCtx, &pb.DumpSpecificationRequest{
		ProjectName: projectName,
		JobName:     jobName,
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
