package deploy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/namespace"
	"github.com/odpf/optimus/cmd/plugin"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/cmd/project"
	"github.com/odpf/optimus/cmd/resource"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
)

const deploymentTimeout = time.Minute * 15

type deployCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	selectedNamespaceNames   []string
	ignoreJobDeployment      bool
	ignoreResourceDeployment bool
	verbose                  bool
	configFilePath           string

	pluginCleanFn func()
}

// NewDeployCommand initializes command for deployment
func NewDeployCommand() *cobra.Command {
	deploy := &deployCommand{}

	cmd := &cobra.Command{
		Use:   "deploy",
		Short: "Deploy current optimus project to server",
		Long: heredoc.Doc(`Apply local changes to destination server which includes creating/updating/deleting
				jobs and creating/updating datastore resources`),
		Example: "optimus deploy [--ignore-resources|--ignore-jobs]",
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE:     deploy.RunE,
		PreRunE:  deploy.PreRunE,
		PostRunE: deploy.PostRunE,
	}
	cmd.Flags().StringVarP(&deploy.configFilePath, "config", "c", deploy.configFilePath, "File path for client configuration")
	cmd.Flags().StringSliceVarP(&deploy.selectedNamespaceNames, "namespace-names", "N", nil, "Selected namespaces of optimus project")
	cmd.Flags().BoolVarP(&deploy.verbose, "verbose", "v", false, "Print details related to deployment stages")
	cmd.Flags().BoolVar(&deploy.ignoreJobDeployment, "ignore-jobs", false, "Ignore deployment of jobs")
	cmd.Flags().BoolVar(&deploy.ignoreResourceDeployment, "ignore-resources", false, "Ignore deployment of resources")
	return cmd
}

func (d *deployCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	var err error
	d.clientConfig, err = config.LoadClientConfig(d.configFilePath, cmd.Flags())
	if err != nil {
		return err
	}
	d.logger = logger.NewClientLogger(d.clientConfig.Log)

	d.logger.Info(logger.ColoredNotice("Initializing client plugins"))
	d.pluginCleanFn, err = plugin.TriggerClientPluginsInit(d.clientConfig.Log.Level)
	d.logger.Info("initialization finished!\n")
	return err
}

func (d *deployCommand) RunE(_ *cobra.Command, _ []string) error {
	d.logger.Info(logger.ColoredNotice("Registering project [%s] to [%s]", d.clientConfig.Project.Name, d.clientConfig.Host))
	if err := project.RegisterProject(d.logger, d.clientConfig.Host, d.clientConfig.Project); err != nil {
		return err
	}
	d.logger.Info("project registration finished!\n")

	d.logger.Info(logger.ColoredNotice("Validating namespaces"))
	selectedNamespaces, err := d.clientConfig.GetSelectedNamespaces(d.selectedNamespaceNames...)
	if err != nil {
		return err
	}
	if len(selectedNamespaces) == 0 {
		selectedNamespaces = d.clientConfig.Namespaces
	}
	d.logger.Info("validation finished!\n")

	d.logger.Info(logger.ColoredNotice("Registering namespaces for [%s] to [%s]", d.clientConfig.Project.Name, d.clientConfig.Host))
	if err := namespace.RegisterSelectedNamespaces(d.logger, d.clientConfig.Host, d.clientConfig.Project.Name, selectedNamespaces...); err != nil {
		return err
	}
	d.logger.Info("namespace registration finished!\n")

	if d.ignoreJobDeployment && d.ignoreResourceDeployment {
		d.logger.Info(logger.ColoredNotice("No jobs and resources to be deployed"))
		return nil
	}
	return d.deploy(selectedNamespaces)
}

func (d *deployCommand) PostRunE(_ *cobra.Command, _ []string) error {
	d.pluginCleanFn()
	return nil
}

func (d *deployCommand) deploy(selectedNamespaces []*config.Namespace) error {
	conn, err := connectivity.NewConnectivity(d.clientConfig.Host, deploymentTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	if !d.ignoreResourceDeployment {
		d.logger.Info(logger.ColoredNotice("> Deploying all resources"))
		if err := d.deployResources(conn, selectedNamespaces); err != nil {
			return err
		}
		d.logger.Info("> resource deployment finished!\n")
	} else {
		d.logger.Info("> Skipping resource deployment")
	}

	if !d.ignoreJobDeployment {
		d.logger.Info(logger.ColoredNotice("> Deploying all jobs"))
		if err := d.deployJobs(conn, selectedNamespaces); err != nil {
			return err
		}
		d.logger.Info("> job deployment finished!\n")
	} else {
		d.logger.Info("> Skipping job deployment")
	}
	return nil
}

func (d *deployCommand) deployJobs(conn *connectivity.Connectivity, selectedNamespaces []*config.Namespace) error {
	stream, err := d.getJobStreamClient(conn)
	if err != nil {
		return err
	}

	var totalSpecsCount int
	for _, namespace := range selectedNamespaces {
		progressFn := func(totalCount int) {
			totalSpecsCount += totalCount
		}
		if err := d.sendNamespaceJobRequest(stream, namespace, progressFn); err != nil {
			if errors.Is(err, models.ErrNoJobs) {
				d.logger.Info(fmt.Sprintf("no job specifications are found for namespace [%s]", namespace.Name))
				continue
			}
			return fmt.Errorf("error getting job specs for namespace [%s]: %w", namespace.Name, err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}

	if totalSpecsCount == 0 {
		d.logger.Warn("no job specs are found from all the namespaces")
		return nil
	}
	return d.processJobDeploymentResponse(stream, totalSpecsCount)
}

func (d *deployCommand) processJobDeploymentResponse(
	stream pb.JobSpecificationService_DeployJobSpecificationClient,
	totalSpecsCount int,
) error {
	d.logger.Info("> Receiving responses:")
	var counter int
	spinner := progressbar.NewProgressBar()
	defer spinner.Stop()

	if !d.verbose {
		spinner.StartProgress(totalSpecsCount, "please wait")
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if resp.GetAck() {
			if !resp.GetSuccess() {
				d.logger.Error(resp.GetMessage())
			}
			if resp.GetJobName() != "" {
				counter++
				spinner.SetProgress(counter)
				if d.verbose {
					d.logger.Info(fmt.Sprintf("[%d/%d] %s successfully deployed", counter, totalSpecsCount, resp.GetJobName()))
				}
			} else if d.verbose {
				d.logger.Info(resp.Message)
			}
		}
	}
	return nil
}

func (d *deployCommand) sendNamespaceJobRequest(
	stream pb.JobSpecificationService_DeployJobSpecificationClient,
	namespace *config.Namespace,
	progressFn func(totalCount int),
) error {
	request, err := d.getJobDeploymentRequest(d.clientConfig.Project.Name, namespace)
	if err != nil {
		return err
	}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("deployment for namespace [%s] failed: %w", namespace.Name, err)
	}
	progressFn(len(request.GetJobs()))
	return nil
}

func (*deployCommand) getJobDeploymentRequest(projectName string, namespace *config.Namespace) (*pb.DeployJobSpecificationRequest, error) {
	pluginRepo := models.PluginRegistry

	jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), namespace.Job.Path)
	jobSpecRepo := local.NewJobSpecRepository(
		jobSpecFs,
		local.NewJobSpecAdapter(pluginRepo),
	)

	jobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return nil, err
	}

	adaptedJobSpecs := make([]*pb.JobSpecification, len(jobSpecs))
	for i, spec := range jobSpecs {
		adaptedJobSpecs[i] = v1handler.ToJobProto(spec)
	}
	return &pb.DeployJobSpecificationRequest{
		Jobs:          adaptedJobSpecs,
		ProjectName:   projectName,
		NamespaceName: namespace.Name,
	}, nil
}

func (d *deployCommand) getJobStreamClient(
	conn *connectivity.Connectivity,
) (pb.JobSpecificationService_DeployJobSpecificationClient, error) {
	client := pb.NewJobSpecificationServiceClient(conn.GetConnection())
	stream, err := client.DeployJobSpecification(conn.GetContext())
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			d.logger.Error(logger.ColoredError("Deployment process took too long, timing out"))
		}
		return nil, fmt.Errorf("deployement failed: %w", err)
	}
	return stream, nil
}

func (d *deployCommand) deployResources(
	conn *connectivity.Connectivity,
	selectedNamespaces []*config.Namespace,
) error {
	stream, err := d.getResourceStreamClient(conn)
	if err != nil {
		return err
	}

	var totalSpecsCount int
	for _, namespace := range selectedNamespaces {
		progressFn := func(totalCount int) {
			totalSpecsCount += totalCount
		}
		if err := d.sendNamespaceResourceRequest(
			conn.GetContext(), stream, namespace, progressFn,
		); err != nil {
			return err
		}
	}

	if err := stream.CloseSend(); err != nil {
		return err
	}

	if totalSpecsCount == 0 {
		d.logger.Warn("no resource specs are found from all the namespaces")
		return nil
	}
	return d.processResourceDeploymentResponse(stream, totalSpecsCount)
}

func (d *deployCommand) processResourceDeploymentResponse(
	stream pb.ResourceService_DeployResourceSpecificationClient,
	totalSpecsCount int,
) error {
	d.logger.Info("> Receiving responses:")
	var counter int
	spinner := progressbar.NewProgressBar()
	defer spinner.Stop()

	if !d.verbose {
		spinner.StartProgress(totalSpecsCount, "please wait")
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if resp.GetAck() {
			if !resp.GetSuccess() {
				d.logger.Error(resp.GetMessage())
			}
			if resp.GetResourceName() != "" {
				counter++
				spinner.SetProgress(counter)
				if d.verbose {
					d.logger.Info(fmt.Sprintf("[%d/%d] %s successfully deployed", counter, totalSpecsCount, resp.GetResourceName()))
				}
			} else if d.verbose {
				d.logger.Info(resp.Message)
			}
		}
	}
	return nil
}

func (d *deployCommand) sendNamespaceResourceRequest(
	ctx context.Context, stream pb.ResourceService_DeployResourceSpecificationClient,
	namespace *config.Namespace, progressFn func(totalCount int),
) error {
	datastoreSpecFs := resource.CreateDataStoreSpecFs(namespace)
	for storeName, repoFS := range datastoreSpecFs {
		d.logger.Info(fmt.Sprintf("> Deploying %s resources for namespace [%s]", storeName, namespace.Name))
		request, err := d.getResourceDeploymentRequest(ctx, namespace.Name, storeName, repoFS)
		if err != nil {
			if errors.Is(err, models.ErrNoResources) {
				d.logger.Info(fmt.Sprintf("no resource specifications are found for namespace [%s]", namespace.Name))
				continue
			}
			return fmt.Errorf("error getting resource specs for namespace [%s]: %w", namespace.Name, err)
		}

		if err := stream.Send(request); err != nil {
			return fmt.Errorf("deployment for namespace [%s] failed: %w", namespace.Name, err)
		}
		progressFn(len(request.GetResources()))
	}
	return nil
}

func (d *deployCommand) getResourceDeploymentRequest(
	ctx context.Context,
	namespaceName, storeName string,
	repoFS afero.Fs,
) (*pb.DeployResourceSpecificationRequest, error) {
	datastoreRepo := models.DatastoreRegistry

	ds, err := datastoreRepo.GetByName(storeName)
	if err != nil {
		return nil, fmt.Errorf("unsupported datastore [%s] for namesapce [%s]", storeName, namespaceName)
	}

	resourceSpecRepo := local.NewResourceSpecRepository(repoFS, ds)
	resourceSpecs, err := resourceSpecRepo.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	adaptedSpecs := make([]*pb.ResourceSpecification, len(resourceSpecs))
	for i, spec := range resourceSpecs {
		adapted, err := v1handler.ToResourceProto(spec)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize [%s] for namespace [%s]: %w", spec.Name, namespaceName, err)
		}
		adaptedSpecs[i] = adapted
	}
	return &pb.DeployResourceSpecificationRequest{
		Resources:     adaptedSpecs,
		ProjectName:   d.clientConfig.Project.Name,
		DatastoreName: storeName,
		NamespaceName: namespaceName,
	}, nil
}

func (d *deployCommand) getResourceStreamClient(
	conn *connectivity.Connectivity,
) (pb.ResourceService_DeployResourceSpecificationClient, error) {
	client := pb.NewResourceServiceClient(conn.GetConnection())
	stream, err := client.DeployResourceSpecification(conn.GetContext())
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			d.logger.Error(logger.ColoredError("Deployment process took too long, timing out"))
		}
		return nil, fmt.Errorf("deployement failed: %w", err)
	}
	return stream, nil
}
