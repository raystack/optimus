package job

import (
	"context"
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

type explainCommand struct {
	logger          log.Logger
	configFilePath  string
	clientConfig    *config.ClientConfig
	jobSurvey       *survey.JobSurvey
	namespaceSurvey *survey.NamespaceSurvey
}

// NewexplainCommand initializes command for explaining job specification
func NewExplainCommand() *cobra.Command {
	explain := &explainCommand{
		clientConfig: &config.ClientConfig{},
	}
	cmd := &cobra.Command{
		Use:     "explain",
		Short:   "Apply template values in job specification to current 'explain' directory", // todo fix this
		Long:    "Process optimus job specification based on macros/functions used.",         // todo fix this
		Example: "optimus job explain [<job_name>]",
		RunE:    explain.RunE,
		PreRunE: explain.PreRunE,
	}

	// Config filepath flag
	cmd.Flags().StringVarP(&explain.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	return cmd
}

func (r *explainCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load mandatory config
	if err := r.loadConfig(); err != nil {
		return err
	}

	r.logger = logger.NewClientLogger(r.clientConfig.Log)
	r.jobSurvey = survey.NewJobSurvey()
	r.namespaceSurvey = survey.NewNamespaceSurvey(r.logger)
	return nil
}

func (r *explainCommand) RunE(_ *cobra.Command, args []string) error {
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
	explainedPath := filepath.Join(".", "explain", jobSpec.Name)
	if err := os.MkdirAll(explainedPath, 0o770); err != nil {
		return err
	}
	r.logger.Info(fmt.Sprintf("Downloading assets in %s", explainedPath))

	now := time.Now()
	r.logger.Info(fmt.Sprintf("Assuming execution time as current time of %s\n", now.Format(models.InstanceScheduledAtTimeLayout)))

	templateEngine := compiler.NewGoEngine()
	templates, err := compiler.DumpAssets(context.Background(), jobSpec, now, templateEngine, true)
	if err != nil {
		return err
	}

	writeToFileFn := utils.WriteStringToFileIndexed()
	for name, content := range templates {
		if err := writeToFileFn(filepath.Join(explainedPath, name), content, r.logger.Writer()); err != nil {
			return err
		}
	}

	r.logger.Info(logger.ColoredSuccess("\nExplain complete."))
	return nil
}

func (r *explainCommand) getJobSpecByName(args []string, namespaceJobPath string) (models.JobSpec, error) {
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

func (r *explainCommand) loadConfig() error {
	// TODO: find a way to load the config in one place
	conf, err := config.LoadClientConfig(r.configFilePath)
	if err != nil {
		return err
	}
	*r.clientConfig = *conf
	return nil
}