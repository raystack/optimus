package commands

import (
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/resources"
	"github.com/odpf/optimus/store"
)

func dumpCommand(l logger, jobSpecRepo store.JobSpecRepository, scheduler models.SchedulerUnit) *cli.Command {
	return &cli.Command{
		Use:   "dump",
		Short: "write the representation of the resource to stdout",
		Args: func(cmd *cli.Command, args []string) error {
			if len(args) < 1 {
				return errors.New("job name is required")
			}
			return nil
		},
		Run: func(c *cli.Command, args []string) {
			jobSpecs, err := jobSpecRepo.GetAll()
			if err != nil {
				panic(err)
			}

			jobSpecs, err = job.NewDependencyResolver().Resolve(jobSpecs)
			if err != nil {
				panic(err)
			}
			jobSpecs, err = job.NewPriorityResolver().Resolve(jobSpecs)
			if err != nil {
				panic(err)
			}

			compiler := job.NewCompiler(resources.FileSystem, scheduler.GetTemplatePath(), "localhost")
			for _, spec := range jobSpecs {
				if spec.Name != args[0] {
					continue
				}

				compiled, err := compiler.Compile(spec, models.ProjectSpec{Name: "local"})
				if err != nil {
					panic(err)
				}
				l.Println(string(compiled.Contents))
			}
		},
	}
}
