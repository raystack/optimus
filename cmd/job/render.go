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
	"github.com/odpf/optimus/core/cron"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
	"github.com/odpf/optimus/utils"
)

type renderCommand struct {
	logger          log.Logger
	configFilePath  string
	clientConfig    *config.ClientConfig
	jobSurvey       *survey.JobSurvey
	namespaceSurvey *survey.NamespaceSurvey
	scheduledAt     string
}

// NewRenderCommand initializes command for rendering job specification
func NewRenderCommand() *cobra.Command {
	render := &renderCommand{
		clientConfig: &config.ClientConfig{},
	}
	cmd := &cobra.Command{
		Use:     "render",
		Short:   "Apply template values in job specification to current 'render' directory",
		Long:    "Process optimus job specification based on macros/functions used.",
		Example: "optimus job render [<job_name>] [--scheduledAt <2006-01-02 15:04>]",
		RunE:    render.RunE,
		PreRunE: render.PreRunE,
	}

	// Config filepath flag
	cmd.Flags().StringVarP(&render.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVarP(&render.scheduledAt, "scheduledAt", "t", "", "Time at which the job is scheduled for execution")
	return cmd
}

func (r *renderCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load mandatory config
	if err := r.loadConfig(); err != nil {
		return err
	}
	r.logger = logger.NewClientLogger(r.clientConfig.Log)
	r.jobSurvey = survey.NewJobSurvey()
	r.namespaceSurvey = survey.NewNamespaceSurvey(r.logger)
	if r.scheduledAt != "" {
		_, err := time.Parse("2006-01-02 15:04", r.scheduledAt)
		if err != nil {
			return err
		}
	}
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
	var scheduleTime time.Time
	if r.scheduledAt != "" {
		scheduleTime, _ = time.Parse("2006-01-02 15:04", r.scheduledAt)
	} else {
		scheduleTime = time.Now()
	}
	startTime := jobSpec.Task.Window.GetStart(scheduleTime)
	endTime := jobSpec.Task.Window.GetEnd(scheduleTime)

	r.logger.Info("job dependencies")
	for dependencyJobName := range jobSpec.Dependencies {
		r.logger.Info("jobName::" + logger.ColoredNotice(dependencyJobName))
		jobSpec, _ := r.getJobSpecByName([]string{dependencyJobName}, namespace.Job.Path)
		// this could be a deployed or an undeployed job
		//check that
		// another concern, if a job is both , then which version to honor ask sravan
		//fmt.Println("jobName::", jobSpec)
		// fmt.Println(jobSpec.Schedule)
		// fmt.Println(jobSpec.Schedule.Interval)
		jobCron, err := cron.ParseCronSchedule(jobSpec.Schedule.Interval)
		if err != nil {
			r.logger.Error(err.Error())
		}
		scheduledTimes := jobCron.GetExpectedRuns(startTime, endTime)
		scheduledTimesCombinedString := ""
		for _, scheduledTime := range scheduledTimes {
			scheduledTimesCombinedString += ("[" + scheduledTime.Format("2006-01-02 15:04:05") + "] ")
		}
		r.logger.Info("execution times -> " + scheduledTimesCombinedString)
	}
	// for _, dependency := range jobSpec.ExternalDependencies.OptimusDependencies {
	//	jobCron, err := cron.ParseCronSchedule(dependency.Job.Schedule.Interval)
	//	scheduledTimes := jobCron.GetExpectedRuns(startTime, endTime)
	//}

	//fmt.Println(jobSpec.ExternalDependencies)
	r.logger.Info("job external dependencies")
	for _, externalDepency := range jobSpec.ExternalDependencies.HTTPDependencies {
		r.logger.Info(externalDepency.Name)
	}
	for _, externalDepency := range jobSpec.ExternalDependencies.OptimusDependencies {
		r.logger.Info(externalDepency.Name)
	}

	// create temporary directory
	renderedPath := filepath.Join(".", "render", jobSpec.Name)
	if err := os.MkdirAll(renderedPath, 0o770); err != nil {
		return err
	}
	r.logger.Info(fmt.Sprintf("Rendering assets in %s", renderedPath))

	r.logger.Info(fmt.Sprintf("Assuming execution time as current time of %s\n", scheduleTime.Format(models.InstanceScheduledAtTimeLayout)))

	templateEngine := compiler.NewGoEngine()
	templates, err := compiler.DumpAssets(context.Background(), jobSpec, scheduleTime, templateEngine, true)
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

func (r *renderCommand) loadConfig() error {
	// TODO: find a way to load the config in one place
	conf, err := config.LoadClientConfig(r.configFilePath)
	if err != nil {
		return err
	}
	*r.clientConfig = *conf
	return nil
}
