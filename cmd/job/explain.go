package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"

	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/survey"
	"github.com/odpf/optimus/compiler"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
	"github.com/odpf/optimus/utils"
)

type explainCommand struct {
	logger log.Logger

	configFilePath string

	projectName   string
	namespaceName string

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

	//cmd.Flags().StringVar(&explain.host, "host", "", "Optimus service endpoint url")

	return cmd
}

func (e *explainCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load mandatory config
	if err := e.loadConfig(); err != nil {
		return err
	}

	e.logger = logger.NewClientLogger(e.clientConfig.Log)
	e.jobSurvey = survey.NewJobSurvey()
	e.namespaceSurvey = survey.NewNamespaceSurvey(e.logger)
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

	e.logger.Info("\n\n\n ********* string(jobSpecFromServerByte) \n\n")

	jobSpecFromServer := e.GetJobSpecFromServer(jobSpec)

	jobSpecFromServerByte, _ := json.Marshal(jobSpecFromServer)

	e.logger.Info(string(jobSpecFromServerByte))
	// create temporary directory
	explainedPath := filepath.Join(".", "explain", jobSpec.Name)
	if err := os.MkdirAll(explainedPath, 0o770); err != nil {
		return err
	}
	e.logger.Info(fmt.Sprintf("Downloading assets in %s", explainedPath))

	now := time.Now()
	e.logger.Info(fmt.Sprintf("Assuming execution time as current time of %s\n", now.Format(models.InstanceScheduledAtTimeLayout)))

	templateEngine := compiler.NewGoEngine()
	templates, err := compiler.DumpAssets(context.Background(), jobSpec, now, templateEngine, true)
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

func (e *explainCommand) GetJobSpecFromServer(jobSpec models.JobSpec) *pb.JobSpecification {
	timeout := time.Minute * 5
	conn, err := connectivity.NewConnectivity(e.clientConfig.Host, timeout)
	if err != nil {
		//todo handle later
		fmt.Println("err:: ", err.Error())
		return nil
	}
	defer conn.Close()

	jobSpecService := pb.NewJobSpecificationServiceClient(conn.GetConnection())
	jobExplainResponse, err := jobSpecService.JobExplain(conn.GetContext(), &pb.JobExplainRequest{
		ProjectName:   e.projectName,
		NamespaceName: e.namespaceName,
		Spec:          v1handler.ToJobSpecificationProto(jobSpec),
	})
	// list dependencies / later do this with dependency type too.
	for dependencyName, dependencySpec := range jobExplainResponse.Dependencies {
		e.logger.Info(logger.ColoredSuccess("\n> Upstream job name:: %v", dependencyName))
		e.logger.Info(logger.ColoredNotice("\nOwner:: %v", dependencySpec.Owner))
		e.logger.Info(logger.ColoredNotice("\nSchedule Interval:: %v, StartDate:: %v, EndDate:: %v", dependencySpec.Interval, dependencySpec.StartDate, dependencySpec.EndDate))
		e.logger.Info(logger.ColoredNotice("\nSchedule Interval:: %v, StartDate:: %v, EndDate:: %v", dependencySpec.Interval, dependencySpec.StartDate, dependencySpec.EndDate))
		e.logger.Info(logger.ColoredNotice("\nWindowSize:: %v, WindowOffset:: %v, WindowTruncateTo:: %v", dependencySpec.WindowSize, dependencySpec.WindowOffset, dependencySpec.WindowTruncateTo))
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			e.logger.Error(logger.ColoredError("Refresh process took too long, timing out"))
		}
		//return fmt.Errorf("refresh request failed: %w", err)
		//todo handle later
		fmt.Println("err:: ", err.Error())
		return nil
	}

	return jobExplainResponse.Spec
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
