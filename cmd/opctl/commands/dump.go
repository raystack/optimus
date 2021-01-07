package commands

import (
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"github.com/odpf/optimus/job"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/resources"
	"github.com/odpf/optimus/store"
)

var (
	schedulerTemplatePath = "./templates/airflow_1/base_dag.py"
)

func dumpCommand(l logger, jobSpecRepo store.JobSpecRepository) *cli.Command {
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
			jobSpec, err := jobSpecRepo.GetByName(args[0])
			if err != nil {
				panic(err)
			}

			specs, err := job.NewDependencyResolver().Resolve([]models.JobSpec{jobSpec})
			if err != nil {
				panic(err)
			}
			compiler := job.NewCompiler(resources.FileSystem, schedulerTemplatePath)
			for _, spec := range specs {
				compiled, err := compiler.Compile(spec)
				if err != nil {
					panic(err)
				}
				l.Println(string(compiled.Contents))
			}
		},
	}
}
