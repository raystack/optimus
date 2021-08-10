package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/run"
	"github.com/odpf/optimus/utils"
	"github.com/odpf/salt/log"
	cli "github.com/spf13/cobra"
)

var (
	templateEngine = run.NewGoEngine()
)

func renderCommand(l log.Logger, host string, jobSpecRepo JobSpecRepository) *cli.Command {
	cmd := &cli.Command{
		Use:   "render",
		Short: "convert raw representation of specification to consumables",
	}
	if jobSpecRepo != nil {
		cmd.AddCommand(renderTemplateCommand(l, jobSpecRepo))
	}
	return cmd
}

func renderTemplateCommand(l log.Logger, jobSpecRepo JobSpecRepository) *cli.Command {
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
		l.Info(fmt.Sprintf("rendering assets in %s", renderedPath))

		now := time.Now()
		l.Info(fmt.Sprintf("assuming execution time as current time of %s", now.Format(models.InstanceScheduledAtTimeLayout)))

		templates, err := run.DumpAssets(jobSpec, now, templateEngine, true)
		if err != nil {
			return err
		}

		writeToFileFn := utils.WriteStringToFileIndexed()
		for name, content := range templates {
			if err := writeToFileFn(filepath.Join(renderedPath, name), content, l.Writer()); err != nil {
				return err
			}
		}

		l.Info(coloredSuccess("render complete"))
		return nil
	}

	return cmd
}
