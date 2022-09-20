package job

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/cmd/internal/survey"
	"github.com/odpf/optimus/client/local"
	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/internal/utils"
	"github.com/odpf/optimus/models"
)

type renderCommand struct {
	logger          log.Logger
	configFilePath  string
	clientConfig    *config.ClientConfig
	jobSurvey       *survey.JobSurvey
	namespaceSurvey *survey.NamespaceSurvey
}

// NewRenderCommand initializes command for rendering job specification
func NewRenderCommand() *cobra.Command {
	l := logger.NewClientLogger()
	render := &renderCommand{
		logger:          l,
		jobSurvey:       survey.NewJobSurvey(),
		namespaceSurvey: survey.NewNamespaceSurvey(l),
	}
	cmd := &cobra.Command{
		Use:     "render",
		Short:   "Apply template values in job specification to current 'render' directory",
		Long:    "Process optimus job specification based on macros/functions used.",
		Example: "optimus job render [<job_name>]",
		RunE:    render.RunE,
		PreRunE: render.PreRunE,
	}

	// Config filepath flag
	cmd.Flags().StringVarP(&render.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	return cmd
}

func (r *renderCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load mandatory config
	conf, err := config.LoadClientConfig(r.configFilePath)
	if err != nil {
		return err
	}

	r.clientConfig = conf
	return nil
}

func (r *renderCommand) RunE(_ *cobra.Command, args []string) error {
	namespace, err := r.namespaceSurvey.AskToSelectNamespace(r.clientConfig)
	if err != nil {
		return err
	}
	// TODO: fetch jobSpec from server instead
	jobSpec, err := r.getJobSpecByName(args, namespace.Job.Path)
	if err != nil {
		return err
	}

	// create temporary directory
	renderedPath := filepath.Join(".", "render", jobSpec.Name)
	if err := os.MkdirAll(renderedPath, 0o770); err != nil {
		return err
	}
	r.logger.Info("Rendering assets in %s", renderedPath)

	now := time.Now()
	r.logger.Info("Assuming execution time as current time of %s\n", now.Format(models.InstanceScheduledAtTimeLayout))

	templateEngine := compiler.NewGoEngine()
	templates, err := compiler.DumpAssets(context.Background(), jobSpec, now, templateEngine, true)
	if err != nil {
		return err
	}

	writeToFileFn := utils.WriteStringToFileIndexed()
	for name, content := range templates {
		if err := writeToFileFn(filepath.Join(renderedPath, name), content, r.logger.Writer()); err != nil {
			return err
		}
	}

	r.logger.Info("\nRender complete.")
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
