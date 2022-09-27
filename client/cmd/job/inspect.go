package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/cmd/internal/survey"
	"github.com/odpf/optimus/client/local"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

const (
	inspectTimeout         = time.Minute * 1
	optimusServerFetchFlag = "server"
)

type inspectCommand struct {
	logger log.Logger

	configFilePath string

	projectName   string
	namespaceName string
	host          string

	clientConfig    *config.ClientConfig
	jobSurvey       *survey.JobSurvey
	namespaceSurvey *survey.NamespaceSurvey
}

// NewInspectCommand initializes command for inspecting job specification
func NewInspectCommand() *cobra.Command {
	inspect := &inspectCommand{
		clientConfig: &config.ClientConfig{},
	}
	cmd := &cobra.Command{
		Use:     "inspect",
		Short:   "inspect optimus job specification using local and server spec",
		Long:    "inspect optimus job specification using local and server spec",
		Example: "optimus job inspect [<job_name>] [--server]",
		Args:    cobra.MinimumNArgs(1),
		RunE:    inspect.RunE,
		PreRunE: inspect.PreRunE,
	}
	cmd.Flags().StringVarP(&inspect.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVar(&inspect.host, "host", "", "Optimus service endpoint url")
	cmd.Flags().Bool(optimusServerFetchFlag, false, "fetch jobSpec from server")
	return cmd
}

func (e *inspectCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load mandatory config
	if err := e.loadConfig(); err != nil {
		return err
	}
	e.logger = logger.NewClientLogger()
	e.jobSurvey = survey.NewJobSurvey()
	e.namespaceSurvey = survey.NewNamespaceSurvey(e.logger)
	return nil
}

func (e *inspectCommand) RunE(cmd *cobra.Command, args []string) error {
	e.projectName = e.clientConfig.Project.Name
	namespace, err := e.namespaceSurvey.AskToSelectNamespace(e.clientConfig)
	e.namespaceName = namespace.Name
	if err != nil {
		return err
	}

	var jobSpec models.JobSpec

	serverFetch, _ := cmd.Flags().GetBool(optimusServerFetchFlag)
	if !serverFetch {
		jobSpec, err = e.getJobSpecByName(args, namespace.Job.Path)
		if err != nil {
			return err
		}
	} else {
		jobSpec.Name = args[0]
	}

	start := time.Now()
	if err := e.inspectJobSpecification(jobSpec, serverFetch); err != nil {
		return err
	}
	e.logger.Info(logger.ColoredSuccess("Jobs inspected successfully, took %s", time.Since(start).Round(time.Second)))
	return nil
}

func (e *inspectCommand) getJobSpecByName(args []string, namespaceJobPath string) (models.JobSpec, error) {
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

func (e *inspectCommand) loadConfig() error {
	conf, err := config.LoadClientConfig(e.configFilePath)
	if err != nil {
		return err
	}
	*e.clientConfig = *conf
	return nil
}

func (e *inspectCommand) inspectJobSpecification(jobSpec models.JobSpec, serverFetch bool) error {
	conn, err := connectivity.NewConnectivity(e.clientConfig.Host, inspectTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	var adaptedSpec *pb.JobSpecification
	var jobName string
	if !serverFetch {
		adaptedSpec = v1handler.ToJobSpecificationProto(jobSpec)
	} else {
		jobName = jobSpec.Name
	}

	jobInspectRequest := &pb.JobInspectRequest{
		ProjectName:   e.clientConfig.Project.Name,
		NamespaceName: e.namespaceName,
		Spec:          adaptedSpec,
		JobName:       jobName,
	}
	job := pb.NewJobSpecificationServiceClient(conn.GetConnection())
	resp, err := job.JobInspect(conn.GetContext(), jobInspectRequest)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			e.logger.Error("Inspect process took too long, timing out")
		}
		return fmt.Errorf("inspect request failed: %w", err)
	}
	return e.processJobInspectResponse(resp)
}

func (e *inspectCommand) processJobInspectResponse(resp *pb.JobInspectResponse) error {
	e.logger.Info("\n> Job Destination:: %v", resp.Destination)
	e.logger.Info("\n> Job Sources:: %v", resp.Sources)
	for i := 0; i < len(resp.Log); i++ {
		switch resp.Log[i].Level {
		case pb.Level_LEVEL_INFO:
			e.logger.Info(fmt.Sprintf("\n> [info] %v", resp.Log[i].Message))
		case pb.Level_LEVEL_WARNING:
			e.logger.Info(logger.ColoredNotice(fmt.Sprintf("\n> [warn] %v", resp.Log[i].Message)))
		case pb.Level_LEVEL_ERROR:
			e.logger.Info(logger.ColoredError(fmt.Sprintf("\n> [error] %v", resp.Log[i].Message)))
		default:
			e.logger.Error(logger.ColoredError(fmt.Sprintf("\nunhandled log level::%v specified with error msg ::%v", resp.Log[i].Level, resp.Log[i].Message)))
		}
	}
	return nil
}
