package job

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/cmd/internal/survey"
	"github.com/odpf/optimus/client/local"
	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/internal/utils"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

const (
	explainTimeout = time.Minute * 1
)

type explainCommand struct {
	logger log.Logger

	configFilePath string

	projectName   string
	namespaceName string
	host          string

	clientConfig    *config.ClientConfig
	jobSurvey       *survey.JobSurvey
	namespaceSurvey *survey.NamespaceSurvey
	scheduleTime    string // see if time is possible directly
}

// NewExplainCommand initializes command for explaining job specification
func NewExplainCommand() *cobra.Command {
	explain := &explainCommand{
		clientConfig: &config.ClientConfig{},
	}
	cmd := &cobra.Command{
		Use:     "inspect",
		Short:   "Apply template values in job specification to current 'explain' directory", // todo fix this
		Long:    "Process optimus job specification based on macros/functions used.",         // todo fix this
		Example: "optimus job inspect [<job_name>]",
		RunE:    explain.RunE,
		PreRunE: explain.PreRunE,
	}

	// Config filepath flag
	cmd.Flags().StringVarP(&explain.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVar(&explain.host, "host", "", "Optimus service endpoint url")
	cmd.Flags().StringVarP(&explain.scheduleTime, "time", "t", "", "schedule time for the job deployment")

	return cmd
}

func (e *explainCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load mandatory config
	if err := e.loadConfig(); err != nil {
		return err
	}

	e.logger = logger.NewClientLogger()
	e.jobSurvey = survey.NewJobSurvey()
	e.namespaceSurvey = survey.NewNamespaceSurvey(e.logger)
	// check if time flag is set and see if schedule time could be parsed
	if e.scheduleTime != "" {
		_, err := time.Parse("2006-01-02 15:04", e.scheduleTime)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *explainCommand) RunE(_ *cobra.Command, args []string) error {
	e.projectName = e.clientConfig.Project.Name
	namespace, err := e.namespaceSurvey.AskToSelectNamespace(e.clientConfig)
	e.namespaceName = namespace.Name
	if err != nil {
		return err
	}
	// TODO: fetch jobSpec from server instead
	jobSpec, err := e.getJobSpecByName(args, namespace.Job.Path)
	if err != nil {
		return err
	}

	explainedPath := filepath.Join(".", "explain", jobSpec.Name)
	if err := os.MkdirAll(explainedPath, 0o770); err != nil {
		return err
	}
	e.logger.Info(fmt.Sprintf("Downloading assets in %s", explainedPath))

	scheduleTime := time.Now()

	e.logger.Info(fmt.Sprintf("Assuming execution time as current time of %s\n", scheduleTime.Format(models.InstanceScheduledAtTimeLayout)))

	templateEngine := compiler.NewGoEngine()
	templates, err := compiler.DumpAssets(context.Background(), jobSpec, scheduleTime, templateEngine, true)
	if err != nil {
		return err
	}

	writeToFileFn := utils.WriteStringToFileIndexed()
	for name, content := range templates {
		if err := writeToFileFn(filepath.Join(explainedPath, name), content, e.logger.Writer()); err != nil {
			return err
		}
	}

	e.logger.Info(logger.ColoredSuccess("\nExplain complete."))
	return nil
}

func (e *explainCommand) getJobSpecByName(args []string, namespaceJobPath string) (models.JobSpec, error) {
	pluginRepo := models.PluginRegistry
	jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), namespaceJobPath)
	jobSpecRepo := local.NewJobSpecRepository(jobSpecFs, local.NewJobSpecAdapter(pluginRepo))

	var jobName string
	var err error
	if len(args) == 0 {
		jobName, err = e.jobSurvey.AskToSelectJobName(jobSpecRepo)
		if err != nil {
			return models.JobSpec{}, err
		}
	} else {
		jobName = args[0]
	}
	return jobSpecRepo.GetByName(jobName)
}

func (e *explainCommand) GetJobSpecFromServer(jobSpec models.JobSpec) *pb.JobInspectResponse {
	conn, err := connectivity.NewConnectivity(e.clientConfig.Host, explainTimeout)
	if err != nil {
		e.logger.Error(logger.ColoredError(err.Error()))
		return nil
	}
	defer conn.Close()

	jobSpecService := pb.NewJobSpecificationServiceClient(conn.GetConnection())
	jobInspectResponse, err := jobSpecService.JobInspect(conn.GetContext(), &pb.JobInspectRequest{
		ProjectName:   e.projectName,
		NamespaceName: e.namespaceName,
		Spec:          v1handler.ToJobSpecificationProto(jobSpec),
	})

	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			e.logger.Error(logger.ColoredError("Refresh process took too long, timing out"))
		}
		e.logger.Error(logger.ColoredError("err:: %v", err.Error()))
		return nil
	}

	return jobInspectResponse
}

func (e *explainCommand) loadConfig() error {
	// TODO: find a way to load the config in one place
	conf, err := config.LoadClientConfig(e.configFilePath)
	if err != nil {
		return err
	}
	*e.clientConfig = *conf
	return nil
}
