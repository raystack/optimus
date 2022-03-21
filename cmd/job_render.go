package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/run"
	"github.com/odpf/optimus/store/local"
	"github.com/odpf/optimus/utils"
	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	cli "github.com/spf13/cobra"
)

func jobRenderTemplateCommand(l log.Logger, conf config.Optimus, pluginRepo models.PluginRepository) *cli.Command {
	var namespaceName string
	cmd := &cli.Command{
		Use:     "render",
		Short:   "Apply template values in job specification to current 'render' directory",
		Long:    "Process optimus job specification based on macros/functions used.",
		Example: "optimus job render [<job_name>]",
		RunE: func(c *cli.Command, args []string) error {
			namespace, err := conf.GetNamespaceByName(namespaceName)
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
			_ = os.MkdirAll(renderedPath, 0770)
			l.Info(fmt.Sprintf("Rendering assets in %s", renderedPath))

			now := time.Now()
			l.Info(fmt.Sprintf("Assuming execution time as current time of %s\n", now.Format(models.InstanceScheduledAtTimeLayout)))

			templateEngine := run.NewGoEngine()
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

			l.Info(coloredSuccess("\nRender complete."))
			return nil
		},
	}
	cmd.Flags().StringVarP(&namespaceName, "namespace", "n", namespaceName, "targeted namespace for renderring template")
	cmd.MarkFlagRequired("namespace")
	return cmd
}
