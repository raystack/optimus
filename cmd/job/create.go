package job

import (
	"path/filepath"
	"strings"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/internal/logger"
	"github.com/odpf/optimus/cmd/internal/survey"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
)

type createCommand struct {
	logger          log.Logger
	configFilePath  string
	clientConfig    *config.ClientConfig
	namespaceSurvey *survey.NamespaceSurvey
	jobCreateSurvey *survey.JobCreateSurvey
}

// NewCreateCommand initializes job create command
func NewCreateCommand() *cobra.Command {
	l := logger.NewClientLogger()
	create := &createCommand{
		logger:          l,
		namespaceSurvey: survey.NewNamespaceSurvey(l),
		jobCreateSurvey: survey.NewJobCreateSurvey(),
	}
	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create a new Job",
		Example: "optimus job create",
		RunE:    create.RunE,
		PreRunE: create.PreRunE,
	}
	// Config filepath flag
	cmd.Flags().StringVarP(&create.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	return cmd
}

func (c *createCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load mandatory config
	conf, err := config.LoadClientConfig(c.configFilePath)
	if err != nil {
		return err
	}

	c.clientConfig = conf
	return nil
}

func (c *createCommand) RunE(_ *cobra.Command, _ []string) error {
	namespace, err := c.namespaceSurvey.AskToSelectNamespace(c.clientConfig)
	if err != nil {
		return err
	}

	jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), namespace.Job.Path)
	jwd, err := survey.AskWorkingDirectory(jobSpecFs, "")
	if err != nil {
		return err
	}

	newDirName, err := survey.AskDirectoryName(jwd)
	if err != nil {
		return err
	}

	jobDirectory := filepath.Join(jwd, newDirName)
	defaultJobName := strings.ReplaceAll(strings.ReplaceAll(jobDirectory, "/", "."), "\\", ".")

	pluginRepo := models.PluginRegistry
	jobSpecAdapter := local.NewJobSpecAdapter(pluginRepo)
	jobSpecRepo := local.NewJobSpecRepository(jobSpecFs, jobSpecAdapter)
	jobInput, err := c.jobCreateSurvey.AskToCreateJob(jobSpecRepo, defaultJobName)
	if err != nil {
		return err
	}

	spec, err := jobSpecAdapter.ToSpec(jobInput)
	if err != nil {
		return err
	}

	if err := jobSpecRepo.SaveAt(spec, jobDirectory); err != nil {
		return err
	}
	c.logger.Info("Job successfully created at %s", jobDirectory)
	return nil
}
