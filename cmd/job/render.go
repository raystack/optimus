package job

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/survey"
	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
	"github.com/odpf/optimus/utils"
)

type renderCommand struct {
	logger          log.Logger
	clientConfig    *config.ClientConfig
	jobSurvey       *survey.JobSurvey
	namespaceSurvey *survey.NamespaceSurvey
}

// NewRenderCommand initializes command for rendering job specification
func NewRenderCommand(clientConfig *config.ClientConfig) *cobra.Command {
	render := &renderCommand{
		clientConfig: clientConfig,
	}
	cmd := &cobra.Command{
		Use:     "render",
		Short:   "Apply template values in job specification to current 'render' directory",
		Long:    "Process optimus job specification based on macros/functions used.",
		Example: "optimus job render [<job_name>]",
		RunE:    render.RunE,
		PreRunE: render.PreRunE,
	}
	return cmd
}

func (r *renderCommand) PreRunE(_ *cobra.Command, _ []string) error {
	r.logger = logger.NewClientLogger(r.clientConfig.Log)
	r.jobSurvey = survey.NewJobSurvey()
	r.namespaceSurvey = survey.NewNamespaceSurvey(r.logger)
	return nil
}

func (r *renderCommand) RunE(_ *cobra.Command, args []string) error {
	namespace, err := r.namespaceSurvey.AskToSelectNamespace(r.clientConfig)
	if err != nil {
		return err
	}
	jobSpec, err := r.getJobSpecByName(args, namespace.Job.Path)
	if err != nil {
		return err
	}

	// create temporary directory
	renderedPath := filepath.Join(".", "render", jobSpec.Name)
	if err := os.MkdirAll(renderedPath, 0o770); err != nil {
		return err
	}
	r.logger.Info(fmt.Sprintf("Rendering assets in %s", renderedPath))

	now := time.Now()
	r.logger.Info(fmt.Sprintf("Assuming execution time as current time of %s\n", now.Format(models.InstanceScheduledAtTimeLayout)))

	templateEngine := compiler.NewGoEngine()
	templates, err := compiler.DumpAssets(jobSpec, now, templateEngine, true)
	if err != nil {
		return err
	}

	writeToFileFn := utils.WriteStringToFileIndexed()
	for name, content := range templates {
		if err := writeToFileFn(filepath.Join(renderedPath, name), content, r.logger.Writer()); err != nil {
			return err
		}
	}

	r.logger.Info(logger.ColoredSuccess("\nRender complete."))
	return nil
}

func (r *renderCommand) getJobSpecByName(args []string, namespaceJobPath string) (models.JobSpec, error) {
	pluginRepo := models.PluginRegistry
	jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), namespaceJobPath)
	jobSpecRepo := local.NewJobSpecRepository(jobSpecFs, local.NewJobSpecAdapter(pluginRepo))

	var jobName string
	var err error
	if len(args) == 0 {
		jobName, err = r.jobSurvey.AskToSelectJobName(jobSpecRepo)
		if err != nil {
			return models.JobSpec{}, err
		}
	} else {
		jobName = args[0]
	}
	return jobSpecRepo.GetByName(jobName)
}
