package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	cli "github.com/spf13/cobra"

	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
	"github.com/odpf/optimus/utils"
)

func jobRenderTemplateCommand(l log.Logger, conf config.ClientConfig, pluginRepo models.PluginRepository) *cli.Command {
	cmd := &cli.Command{
		Use:     "render",
		Short:   "Apply template values in job specification to current 'render' directory",
		Long:    "Process optimus job specification based on macros/functions used.",
		Example: "optimus job render [<job_name>]",
		RunE: func(c *cli.Command, args []string) error {
			namespace, err := askToSelectNamespace(l, conf)
			if err != nil {
				return err
			}
			jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), namespace.Job.Path)
			jobSpecRepo := local.NewJobSpecRepository(
				jobSpecFs,
				local.NewJobSpecAdapter(pluginRepo),
			)
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
			jobSpec, err := jobSpecRepo.GetByName(jobName)
			if err != nil {
				return err
			}

			// create temporary directory
			renderedPath := filepath.Join(".", "render", jobSpec.Name)
			_ = os.MkdirAll(renderedPath, 0o770)
			l.Info(fmt.Sprintf("Rendering assets in %s", renderedPath))

			now := time.Now()
			l.Info(fmt.Sprintf("Assuming execution time as current time of %s\n", now.Format(models.InstanceScheduledAtTimeLayout)))

			templateEngine := compiler.NewGoEngine()
			templates, err := compiler.DumpAssets(jobSpec, now, templateEngine, true)
			if err != nil {
				return err
			}

			writeToFileFn := utils.WriteStringToFileIndexed()
			for name, content := range templates {
				if err := writeToFileFn(filepath.Join(renderedPath, name), content, l.Writer()); err != nil {
					return err
				}
			}

			l.Info(coloredSuccess("\nRender complete."))
			return nil
		},
	}
	return cmd
}
