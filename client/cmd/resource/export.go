package resource

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
	fetchTenantTimeout   = time.Minute
	fetchResourceTimeout = time.Minute * 15
)

type exportCommand struct {
	logger log.Logger
	writer local.SpecWriter[*model.ResourceSpec]

	configFilePath string
	outputDirPath  string
	host           string

	projectName   string
	namespaceName string
	resourceName  string

	storeName string
}

func NewExportCommand() *cobra.Command {
	l := logger.NewClientLogger()
	export := &exportCommand{
		logger:    l,
		storeName: "bigquery",
	}

	cmd := &cobra.Command{
		Use:     "export",
		Short:   "Export resources to YAML files",
		Example: "optimus resource export",
		RunE:    export.RunE,
		PreRunE: export.PreRunE,
	}

	cmd.Flags().StringVarP(&export.configFilePath, "config", "c", export.configFilePath, "File path for client configuration")
	cmd.Flags().StringVar(&export.outputDirPath, "dir", "", "Output directory path")
	cmd.Flags().StringVar(&export.host, "host", "", "Host of the server source (will override value from config)")

	cmd.Flags().StringVarP(&export.projectName, "project-name", "p", "", "Project name target")
	cmd.Flags().StringVarP(&export.namespaceName, "namespace-name", "n", "", "Namespace name target within the selected project name")
	cmd.Flags().StringVarP(&export.resourceName, "resource-name", "r", "", "Resource name target")

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

	readWriter, err := specio.NewResourceSpecReadWriter(afero.NewOsFs())
	e.writer = readWriter
	return err
}

func (e *exportCommand) RunE(_ *cobra.Command, _ []string) error {
	if err := e.validate(); err != nil {
		return err
	}

	var success bool
	if e.projectName != "" && e.namespaceName != "" && e.resourceName != "" {
		success = e.downloadSpecificResource(e.projectName, e.namespaceName, e.resourceName)
	} else if e.projectName != "" && e.namespaceName != "" {
		success = e.downloadByProjectNameAndNamespaceName(e.projectName, e.namespaceName)
	} else if e.projectName != "" {
		success = e.downloadByProjectName(e.projectName)
	} else {
		success = e.downloadAll()
	}

	if !success {
		return errors.New("encountered one or more errors during download resources")
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
	namespaceNames, err := e.fetchNamespaceNames(projectName)
	if err != nil {
		e.logger.Error("error is encountered when fetching namespace names for project [%s]: %s", projectName, err)
		return false
	}

	success := true
	for _, nName := range namespaceNames {
		if !e.downloadByProjectNameAndNamespaceName(projectName, nName) {
			success = false
		}
	}
	return success
}

func (e *exportCommand) downloadByProjectNameAndNamespaceName(projectName, namespaceName string) bool {
	resources, err := e.fetchAllResources(projectName, namespaceName)
	if err != nil {
		e.logger.Error("error is encountered when fetching resource for project [%s] namespace [%s]: %s", projectName, namespaceName, err)
		return false
	}
	if err := e.writeResources(projectName, namespaceName, resources); err != nil {
		e.logger.Error(err.Error())
		return false
	}
	return true
}

func (e *exportCommand) downloadSpecificResource(projectName, namespaceName, resourceName string) bool {
	resource, err := e.fetchSpecificResource(projectName, namespaceName, resourceName)
	if err != nil {
		e.logger.Error("error is encountered when fetching resource for project [%s] namespace [%s]: %s", projectName, namespaceName, err)
		return false
	}
	if err := e.writeResources(projectName, namespaceName, []*model.ResourceSpec{resource}); err != nil {
		e.logger.Error(err.Error())
		return false
	}
	return true
}

func (e *exportCommand) writeResources(projectName, namespaceName string, resources []*model.ResourceSpec) error {
	var errMsgs []string
	for _, res := range resources {
		dirPath := path.Join(e.outputDirPath, projectName, namespaceName, "resources", e.storeName, res.Name)
		if err := e.writer.Write(dirPath, res); err != nil {
			errMsgs = append(errMsgs, err.Error())
		}
	}
	if len(errMsgs) > 0 {
		return fmt.Errorf("encountered one or more errors when writing resources:\n%s", strings.Join(errMsgs, "\n"))
	}
	return nil
}

func (e *exportCommand) fetchAllResources(projectName, namespaceName string) ([]*model.ResourceSpec, error) {
	conn, err := connectivity.NewConnectivity(e.host, fetchResourceTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	resourceServiceClient := pb.NewResourceServiceClient(conn.GetConnection())

	response, err := resourceServiceClient.ListResourceSpecification(conn.GetContext(), &pb.ListResourceSpecificationRequest{
		ProjectName:   projectName,
		NamespaceName: namespaceName,
		DatastoreName: e.storeName,
	})
	if err != nil {
		return nil, err
	}

	output := make([]*model.ResourceSpec, len(response.Resources))
	for i, resource := range response.Resources {
		output[i] = &model.ResourceSpec{
			Version: int(resource.GetVersion()),
			Name:    resource.GetName(),
			Type:    resource.GetType(),
			Labels:  resource.GetLabels(),
			Spec:    resource.GetSpec().AsMap(),
		}
	}
	return output, nil
}

func (e *exportCommand) fetchSpecificResource(projectName, namespaceName, resourceName string) (*model.ResourceSpec, error) {
	conn, err := connectivity.NewConnectivity(e.host, fetchResourceTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	resourceServiceClient := pb.NewResourceServiceClient(conn.GetConnection())

	response, err := resourceServiceClient.ReadResource(conn.GetContext(), &pb.ReadResourceRequest{
		ProjectName:   projectName,
		NamespaceName: namespaceName,
		ResourceName:  resourceName,
		DatastoreName: e.storeName,
	})
	if err != nil {
		return nil, err
	}
	return &model.ResourceSpec{
		Version: int(response.GetResource().Version),
		Name:    response.Resource.GetName(),
		Type:    response.GetResource().Type,
		Labels:  response.Resource.GetLabels(),
		Spec:    response.GetResource().Spec.AsMap(),
	}, nil
}

func (e *exportCommand) fetchNamespaceNames(projectName string) ([]string, error) {
	conn, err := connectivity.NewConnectivity(e.host, fetchTenantTimeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	namespaceServiceClient := pb.NewNamespaceServiceClient(conn.GetConnection())

	response, err := namespaceServiceClient.ListProjectNamespaces(conn.GetContext(), &pb.ListProjectNamespacesRequest{
		ProjectName: projectName,
	})
	if err != nil {
		return nil, err
	}

	output := make([]string, len(response.Namespaces))
	for i, n := range response.Namespaces {
		output[i] = n.GetName()
	}
	return output, nil
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
	if e.resourceName != "" && (e.projectName == "" || e.namespaceName == "") {
		return errors.New("project name and namespace name have to be specified since resource name is specified")
	}
	return nil
}
