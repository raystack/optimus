package job

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"

	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/cmd/internal/survey"
	"github.com/odpf/optimus/client/local/model"
	"github.com/odpf/optimus/client/local/specio"
	"github.com/odpf/optimus/config"
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
		jobSpec.Name = args[0]
	}

	start := time.Now()
	if err := e.inspectJobSpecification(jobSpec, serverFetch); err != nil {
		return err
	}
	e.logger.Info(logger.ColoredSuccess("\nJobs inspected successfully, took %s", time.Since(start).Round(time.Second)))
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
	conn, err := connectivity.NewConnectivity(e.clientConfig.Host, inspectTimeout)
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

func (e *inspectCommand) printLogs(logs []*pb.Log) {
	for i := 0; i < len(logs); i++ {
		switch logs[i].Level {
		case pb.Level_LEVEL_INFO:
			e.logger.Info(fmt.Sprintf("> [info] %v", logs[i].Message))
		case pb.Level_LEVEL_WARNING:
			e.logger.Info(logger.ColoredNotice(fmt.Sprintf("> [warn] %v", logs[i].Message)))
		case pb.Level_LEVEL_ERROR:
			e.logger.Info(logger.ColoredError(fmt.Sprintf("> [error] %v", logs[i].Message)))
		default:
			e.logger.Error(logger.ColoredError(fmt.Sprintf("unhandled log level::%v specified with error msg ::%v", logs[i].Level, logs[i].Message)))
		}
	}
}

func (e *inspectCommand) displayBasicInfoSection(basicInfoSection *pb.JobInspectResponse_BasicInfoSection) {
	e.logger.Info(logger.ColoredNotice("\n-----------------------------------------------------------------------------"))
	e.logger.Info(logger.ColoredNotice("\n    * BASIC INFO"))
	e.logger.Info(logger.ColoredNotice("\n-----------------------------------------------------------------------------"))

	e.logger.Info("\n> Job Destination:: %v", basicInfoSection.Destination)

	e.logger.Info("\n> Job Sources::")
	e.yamlPrint(basicInfoSection.Source)

	e.logger.Info("\n> Job Spec::")
	e.yamlPrint(basicInfoSection.Job)

	e.printLogs(basicInfoSection.Notice)
}

func (e *inspectCommand) processJobInspectResponse(resp *pb.JobInspectResponse) error {
	logger.InitializeColor()
	e.displayBasicInfoSection(resp.BasicInfo)
	e.logger.Info(logger.ColoredNotice("\n-----------------------------------------------------------------------------"))

	return nil
}

func (e *inspectCommand) yamlPrint(input interface{}) {
	marshalled, err := yaml.Marshal(input)
	if err != nil {
		e.logger.Error(fmt.Sprintf("\n Error marshalling %v", input))
	}
	e.logger.Info("%v", string(marshalled))
}
