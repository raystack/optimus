package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/raystack/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	"github.com/raystack/optimus/client/cmd/internal/connection"
	"github.com/raystack/optimus/client/cmd/internal/logger"
	"github.com/raystack/optimus/client/cmd/internal/survey"
	"github.com/raystack/optimus/client/local/model"
	"github.com/raystack/optimus/client/local/specio"
	"github.com/raystack/optimus/config"
	pb "github.com/raystack/optimus/protos/raystack/optimus/core/v1beta1"
)

const (
	inspectTimeout         = time.Minute * 1
	optimusServerFetchFlag = "server"
	MASKED                 = "<masked>"
)

type inspectCommand struct {
	logger     log.Logger
	connection connection.Connection

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
		Short:   "Inspect optimus job specification using local or server spec",
		Long:    "Inspect optimus job specification using local or server spec, Inspect provides dependency run informations and basic validations on Job config",
		Example: "optimus job inspect [<job_name>] [--server]",
		Args:    cobra.MinimumNArgs(1),
		RunE:    inspect.RunE,
		PreRunE: inspect.PreRunE,
	}
	cmd.Flags().StringVarP(&inspect.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	cmd.Flags().StringVar(&inspect.host, "host", "", "Optimus service endpoint url")
	cmd.Flags().Bool(optimusServerFetchFlag, false, "Fetch jobSpec from server")
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

	e.connection = connection.New(e.logger, e.clientConfig)
	return nil
}

func (e *inspectCommand) RunE(cmd *cobra.Command, args []string) error {
	e.projectName = e.clientConfig.Project.Name
	namespace, err := e.namespaceSurvey.AskToSelectNamespace(e.clientConfig)
	e.namespaceName = namespace.Name
	if err != nil {
		return err
	}

	var jobSpec *model.JobSpec
	serverFetch, _ := cmd.Flags().GetBool(optimusServerFetchFlag)
	if !serverFetch {
		jobSpec, err = e.getJobSpecByName(args, namespace.Job.Path)
		if err != nil {
			return err
		}
	} else {
		jobSpec = &model.JobSpec{
			Name: args[0],
		}
	}

	start := time.Now()
	if err := e.inspectJobSpecification(jobSpec, serverFetch); err != nil {
		return err
	}
	e.logger.Info("\nJobs inspected successfully, took %s", time.Since(start).Round(time.Second))
	return nil
}

func (e *inspectCommand) getJobSpecByName(args []string, namespaceJobPath string) (*model.JobSpec, error) {
	jobSpecReadWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs(), specio.WithJobSpecParentReading())
	if err != nil {
		return nil, err
	}
	var jobName string
	if len(args) == 0 {
		jobName, err = e.jobSurvey.AskToSelectJobName(jobSpecReadWriter, namespaceJobPath)
		if err != nil {
			return nil, err
		}
	} else {
		jobName = args[0]
	}
	return jobSpecReadWriter.ReadByName(namespaceJobPath, jobName)
}

func (e *inspectCommand) loadConfig() error {
	conf, err := config.LoadClientConfig(e.configFilePath)
	if err != nil {
		return err
	}
	*e.clientConfig = *conf
	return nil
}

func (e *inspectCommand) inspectJobSpecification(jobSpec *model.JobSpec, serverFetch bool) error {
	conn, err := e.connection.Create(e.clientConfig.Host)
	if err != nil {
		return err
	}
	defer conn.Close()

	var adaptedSpec *pb.JobSpecification
	var jobName string
	if !serverFetch {
		adaptedSpec = jobSpec.ToProto()
	} else {
		jobName = jobSpec.Name
	}

	jobInspectRequest := &pb.JobInspectRequest{
		ProjectName:   e.clientConfig.Project.Name,
		NamespaceName: e.namespaceName,
		Spec:          adaptedSpec,
		JobName:       jobName,
	}
	job := pb.NewJobSpecificationServiceClient(conn)

	ctx, dialCancel := context.WithTimeout(context.Background(), inspectTimeout)
	defer dialCancel()
	resp, err := job.JobInspect(ctx, jobInspectRequest)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			e.logger.Error("Inspect process took too long, timing out")
		}
		return fmt.Errorf("inspect request failed: %w", err)
	}
	return e.processJobInspectResponse(resp)
}

func (e *inspectCommand) printLogs(logs []*pb.Log) {
	for i := 0; i < len(logs); i++ {
		switch logs[i].Level {
		case pb.Level_LEVEL_INFO:
			e.logger.Info(fmt.Sprintf("> [info] %v", logs[i].Message))
		case pb.Level_LEVEL_WARNING:
			e.logger.Warn(fmt.Sprintf("> [warn] %v", logs[i].Message))
		case pb.Level_LEVEL_ERROR:
			e.logger.Error(fmt.Sprintf("> [error] %v", logs[i].Message))
		default:
			e.logger.Error(fmt.Sprintf("unhandled log level::%v specified with error msg::%v", logs[i].Level, logs[i].Message))
		}
	}
}

func getRunsDateArray(jobRunProtos []*pb.JobRun) []string {
	var runsDateArray []string
	for _, run := range jobRunProtos {
		runsDateArray = append(runsDateArray, fmt.Sprintf("%s : %s", run.ScheduledAt.AsTime().Format(time.RFC3339), run.State))
	}
	return runsDateArray
}

func (e *inspectCommand) displayUpstreamSection(upstreams *pb.JobInspectResponse_UpstreamSection) {
	e.logger.Warn("\n-----------------------------------------------------------------------------")
	e.logger.Warn("\n    * UPSTREAMS")
	e.logger.Warn("\n-----------------------------------------------------------------------------")

	e.logger.Info("\n> Internal::")
	var internalDepsArray []map[string]interface{}
	for _, dependency := range upstreams.InternalDependency {
		internalDepsArray = append(internalDepsArray, map[string]interface{}{
			"Job":       fmt.Sprintf("%s/%s", dependency.ProjectName, dependency.Name),
			"Namespace": dependency.NamespaceName,
			"Runs":      getRunsDateArray(dependency.Runs),
			"Task":      dependency.TaskName,
		})
	}
	e.yamlPrint(internalDepsArray)

	e.logger.Info("> External::")
	var externalDepsArray []map[string]interface{}
	for _, dependency := range upstreams.ExternalDependency {
		externalDepsArray = append(externalDepsArray, map[string]interface{}{
			"Job":       fmt.Sprintf("%s/%s", dependency.ProjectName, dependency.Name),
			"Host":      dependency.Host,
			"Namespace": dependency.NamespaceName,
			"Runs":      getRunsDateArray(dependency.Runs),
			"Task":      dependency.TaskName,
		})
	}
	e.yamlPrint(externalDepsArray)

	e.logger.Info("> HTTP::")
	e.yamlPrint(upstreams.HttpDependency)

	e.logger.Info("> Unknown dependencies ::")
	e.yamlPrint(upstreams.UnknownDependencies)

	e.printLogs(upstreams.Notice)
}

func (e *inspectCommand) displayDownstreamSection(downStreams *pb.JobInspectResponse_DownstreamSection) {
	e.logger.Warn("\n-----------------------------------------------------------------------------")
	e.logger.Warn("\n    * DownStream Jobs")
	e.logger.Warn("\n-----------------------------------------------------------------------------")

	e.logger.Info("\n> DownstreamJobs::")
	var downstreamList []map[string]interface{}
	for _, dependency := range downStreams.DownstreamJobs {
		downstreamList = append(downstreamList, map[string]interface{}{
			"Job":       fmt.Sprintf("%s/%s", dependency.ProjectName, dependency.Name),
			"Namespace": dependency.NamespaceName,
			"Task":      dependency.TaskName,
		})
	}
	e.yamlPrint(downstreamList)

	e.printLogs(downStreams.Notice)
}

func (e *inspectCommand) displayBasicInfoSection(basicInfoSection *pb.JobInspectResponse_BasicInfoSection) {
	e.logger.Warn("\n-----------------------------------------------------------------------------")
	e.logger.Warn("\n    * BASIC INFO")
	e.logger.Warn("\n-----------------------------------------------------------------------------")

	e.logger.Info("\n> Job Destination:: %v", basicInfoSection.Destination)

	e.logger.Info("\n> Job Sources::")
	e.yamlPrint(basicInfoSection.Source)

	e.logger.Info("\n> Job Spec::")
	for key := range basicInfoSection.Job.Assets {
		basicInfoSection.Job.Assets[key] = MASKED
	}
	e.yamlPrint(basicInfoSection.Job)

	e.printLogs(basicInfoSection.Notice)
}

func (e *inspectCommand) processJobInspectResponse(resp *pb.JobInspectResponse) error {
	e.displayBasicInfoSection(resp.BasicInfo)
	e.displayUpstreamSection(resp.Upstreams)
	e.displayDownstreamSection(resp.Downstreams)
	e.logger.Warn("\n-----------------------------------------------------------------------------")
	return nil
}

func (e *inspectCommand) yamlPrint(input interface{}) {
	marshalled, err := yaml.Marshal(input)
	if err != nil {
		e.logger.Error(fmt.Sprintf("\n Error marshalling %v", input))
	}
	e.logger.Info("%v", string(marshalled))
}
