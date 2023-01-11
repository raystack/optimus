package job

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/local"
	"github.com/odpf/optimus/client/local/model"
	"github.com/odpf/optimus/client/local/specio"
	"github.com/odpf/optimus/config"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

const (
	fetchTenantTimeout = time.Minute
	fetchJobTimeout    = time.Minute * 15
)

type exportCommand struct {
	logger log.Logger
	writer local.SpecWriter[*model.JobSpec]

	configFilePath string
	outputDirPath  string
	host           string

	projectName   string
	namespaceName string
	jobName       string
}

// NewExportCommand initializes command for exporting job specification to yaml file
func NewExportCommand() *cobra.Command {
	export := &exportCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "export",
		Short:   "Export job specifications to YAML files",
		Example: "optimus job export",
		RunE:    export.RunE,
		PreRunE: export.PreRunE,
	}

	cmd.Flags().StringVarP(&export.configFilePath, "config", "c", export.configFilePath, "File path for client configuration")
	cmd.Flags().StringVar(&export.outputDirPath, "dir", "", "Output directory path")
	cmd.Flags().StringVar(&export.host, "host", "", "Host of the server source (will override value from config)")

	cmd.Flags().StringVarP(&export.projectName, "project-name", "p", "", "Project name target")
	cmd.Flags().StringVarP(&export.namespaceName, "namespace-name", "n", "", "Namespace name target within the selected project name")
	cmd.Flags().StringVarP(&export.jobName, "job-name", "r", "", "Job name target")

	cmd.MarkFlagRequired("dir")
	return cmd
}

func (e *exportCommand) PreRunE(_ *cobra.Command, _ []string) error {
	if e.host != "" {
		return nil
	}
	if e.configFilePath == "" {
		return nil
	}

	cfg, err := config.LoadClientConfig(e.configFilePath)
	if err != nil {
		e.logger.Warn("error is encountered when reading config file: %s", err)
	} else {
		e.host = cfg.Host
	}

	readWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs())
	e.writer = readWriter
	return err
}

func (e *exportCommand) RunE(_ *cobra.Command, _ []string) error {
	if err := e.validate(); err != nil {
		return err
	}

	var success bool
	if e.projectName != "" && e.namespaceName != "" && e.jobName != "" {
		success = e.downloadSpecificJob(e.projectName, e.namespaceName, e.jobName)
	} else if e.projectName != "" && e.namespaceName != "" {
		success = e.downloadByProjectNameAndNamespaceName(e.projectName, e.namespaceName)
	} else if e.projectName != "" {
		success = e.downloadByProjectName(e.projectName)
	} else {
		success = e.downloadAll()
	}

	if !success {
		return errors.New("encountered one or more errors during download jobs")
	}
	return nil
}

func (e *exportCommand) downloadAll() bool {
	projectNames, err := e.fetchProjectNames()
	if err != nil {
		e.logger.Error("error is encountered when fetching project names: %s", err)
		return false
	}
	if len(projectNames) == 0 {
		e.logger.Warn("no project is found from the specified host")
		return true
	}

	success := true
	for _, pName := range projectNames {
		if !e.downloadByProjectName(pName) {
			success = false
		}
	}
	return success
}

func (e *exportCommand) downloadByProjectName(projectName string) bool {
	namespaceJobs, err := e.fetchNamespaceJobsByProjectName(projectName)
	if err != nil {
		e.logger.Error("error is encountered when fetching job specs for project [%s]: %s", projectName, err)
		return false
	}

	for namespaceName, jobSpecs := range namespaceJobs {
		if err := e.writeJobs(projectName, namespaceName, jobSpecs); err != nil {
			e.logger.Error(err.Error())
			return false
		}
	}
	return true
}

func (e *exportCommand) downloadByProjectNameAndNamespaceName(projectName, namespaceName string) bool {
	jobs, err := e.fetchJobsByProjectAndNamespaceName(projectName, namespaceName)
	if err != nil {
		e.logger.Error("error is encountered when fetching job specs for project [%s]: %s", projectName, err)
		return false
	}

	if err := e.writeJobs(projectName, namespaceName, jobs); err != nil {
		e.logger.Error(err.Error())
		return false
	}
	return true
}

func (e *exportCommand) downloadSpecificJob(projectName, namespaceName, jobName string) bool {
	job, err := e.fetchSpecificJob(projectName, namespaceName, jobName)
	if err != nil {
		e.logger.Error("error is encountered when fetching job specs for project [%s]: %s", projectName, err)
		return false
	}

	if err := e.writeJobs(projectName, namespaceName, []*model.JobSpec{job}); err != nil {
		e.logger.Error(err.Error())
		return false
	}
	return true
}

func (e *exportCommand) writeJobs(projectName, namespaceName string, jobs []*model.JobSpec) error {
	var errMsgs []string
	for _, spec := range jobs {
		dirPath := path.Join(e.outputDirPath, projectName, namespaceName, "jobs", spec.Name)
		e.logger.Info("exporting to: " + dirPath)
		if err := e.writer.Write(dirPath, spec); err != nil {
			errMsgs = append(errMsgs, err.Error())
		}
	}
	if len(errMsgs) > 0 {
		return fmt.Errorf("encountered one or more errors when writing jobs:\n%s", strings.Join(errMsgs, "\n"))
	}
	return nil
}

func (e *exportCommand) fetchNamespaceJobsByProjectName(projectName string) (map[string][]*model.JobSpec, error) {
	conn, err := connectivity.NewConnectivity(e.host, fetchJobTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	jobSpecificationServiceClient := pb.NewJobSpecificationServiceClient(conn.GetConnection())

	response, err := jobSpecificationServiceClient.GetJobSpecifications(conn.GetContext(), &pb.GetJobSpecificationsRequest{
		ProjectName: projectName,
	})
	if err != nil {
		return nil, err
	}

	namespaceJobsMap := make(map[string][]*model.JobSpec)
	for _, jobProto := range response.JobSpecificationResponses {
		namespaceJobsMap[jobProto.GetNamespaceName()] = append(namespaceJobsMap[jobProto.GetNamespaceName()], model.ToJobSpec(jobProto.Job))
	}
	return namespaceJobsMap, nil
}

func (e *exportCommand) fetchJobsByProjectAndNamespaceName(projectName, namespaceName string) ([]*model.JobSpec, error) {
	conn, err := connectivity.NewConnectivity(e.host, fetchJobTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	jobSpecificationServiceClient := pb.NewJobSpecificationServiceClient(conn.GetConnection())

	response, err := jobSpecificationServiceClient.GetJobSpecifications(conn.GetContext(), &pb.GetJobSpecificationsRequest{
		ProjectName:   projectName,
		NamespaceName: namespaceName,
	})
	if err != nil {
		return nil, err
	}

	jobs := make([]*model.JobSpec, len(response.JobSpecificationResponses))
	for i, jobProto := range response.JobSpecificationResponses {
		jobs[i] = model.ToJobSpec(jobProto.Job)
	}
	return jobs, nil
}

func (e *exportCommand) fetchSpecificJob(projectName, namespaceName, jobName string) (*model.JobSpec, error) {
	conn, err := connectivity.NewConnectivity(e.host, fetchJobTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	jobSpecificationServiceClient := pb.NewJobSpecificationServiceClient(conn.GetConnection())

	response, err := jobSpecificationServiceClient.GetJobSpecifications(conn.GetContext(), &pb.GetJobSpecificationsRequest{
		ProjectName:   projectName,
		NamespaceName: namespaceName,
		JobName:       jobName,
	})
	if err != nil {
		return nil, err
	}

	if len(response.JobSpecificationResponses) == 0 {
		return nil, errors.New("job is not found")
	}
	return model.ToJobSpec(response.JobSpecificationResponses[0].Job), nil
}

func (e *exportCommand) fetchProjectNames() ([]string, error) {
	conn, err := connectivity.NewConnectivity(e.host, fetchTenantTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	projectServiceClient := pb.NewProjectServiceClient(conn.GetConnection())

	response, err := projectServiceClient.ListProjects(conn.GetContext(), &pb.ListProjectsRequest{})
	if err != nil {
		return nil, err
	}

	output := make([]string, len(response.Projects))
	for i, p := range response.Projects {
		output[i] = p.GetName()
	}
	return output, nil
}

func (e *exportCommand) validate() error {
	if e.host == "" {
		return errors.New("host is not specified in both config file and flag argument")
	}
	if e.namespaceName != "" && e.projectName == "" {
		return errors.New("project name has to be specified since namespace name is specified")
	}
	if e.jobName != "" && (e.projectName == "" || e.namespaceName == "") {
		return errors.New("project name and namespace name have to be specified since job name is specified")
	}
	return nil
}
