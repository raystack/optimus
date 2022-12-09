package deploy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/multierr"

	"github.com/odpf/optimus/client/cmd/internal"
	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/cmd/namespace"
	"github.com/odpf/optimus/client/cmd/project"
	"github.com/odpf/optimus/client/cmd/resource"
	"github.com/odpf/optimus/client/local/specio"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/internal/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

const (
	deployTimeout = time.Minute * 30
	pollInterval  = time.Second * 15

	deploymentCancelled  = "Cancelled"
	deploymentInQueue    = "In Queue"
	deploymentInProgress = "In Progress"
	deploymentSucceed    = "Succeed"
	deploymentFailed     = "Failed"
)

type deployCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	selectedNamespaceNames   []string
	ignoreJobDeployment      bool
	ignoreResourceDeployment bool
	verbose                  bool
	configFilePath           string

	pluginRepo *models.RegisteredPlugins
}

// NewDeployCommand initializes command for deployment
func NewDeployCommand() *cobra.Command {
	deploy := &deployCommand{
		logger: logger.NewClientLogger(),
	}

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

func (d *deployCommand) PreRunE(_ *cobra.Command, _ []string) error {
	var err error
	d.clientConfig, err = config.LoadClientConfig(d.configFilePath)
	if err != nil {
		return err
	}

	d.logger.Info("Initializing client plugins")
	d.pluginRepo, err = internal.InitPlugins(d.clientConfig.Log.Level)
	d.logger.Info("initialization finished!\n")
	return err
}

func (d *deployCommand) RunE(_ *cobra.Command, _ []string) error {
	d.logger.Info("Registering project [%s] to [%s]", d.clientConfig.Project.Name, d.clientConfig.Host)
	if err := project.RegisterProject(d.logger, d.clientConfig.Host, d.clientConfig.Project); err != nil {
		return err
	}
	d.logger.Info("project registration finished!\n")

	d.logger.Info("Validating namespaces")
	selectedNamespaces, err := d.clientConfig.GetSelectedNamespaces(d.selectedNamespaceNames...)
	if err != nil {
		return err
	}
	if len(selectedNamespaces) == 0 {
		selectedNamespaces = d.clientConfig.Namespaces
	}
	d.logger.Info("validation finished!\n")

	d.logger.Info("Registering namespaces for [%s] to [%s]", d.clientConfig.Project.Name, d.clientConfig.Host)
	if err := namespace.RegisterSelectedNamespaces(d.logger, d.clientConfig.Host, d.clientConfig.Project.Name, selectedNamespaces...); err != nil {
		return err
	}
	d.logger.Info("namespace registration finished!\n")

	return d.deploy(selectedNamespaces)
}

func (*deployCommand) PostRunE(*cobra.Command, []string) error {
	internal.CleanupPlugins()
	return nil
}

func (d *deployCommand) deploy(selectedNamespaces []*config.Namespace) error {
	conn, err := connectivity.NewConnectivity(d.clientConfig.Host, deployTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := d.deployResources(conn, selectedNamespaces); err != nil {
		return err
	}
	d.logger.Info("> resource deployment finished!\n")

	if err := d.deployJobs(conn, selectedNamespaces); err != nil {
		return err
	}
	d.logger.Info("> job deployment finished!\n")

	return nil
}

func (d *deployCommand) deployJobs(conn *connectivity.Connectivity, selectedNamespaces []*config.Namespace) error {
	if d.ignoreJobDeployment {
		d.logger.Warn("> Skipping job deployment")
		return nil
	}

	namespaceNames := []string{}
	for _, namespace := range selectedNamespaces {
		namespaceNames = append(namespaceNames, namespace.Name)
	}
	d.logger.Info("\n> Deploying jobs from namespaces [%s]", strings.Join(namespaceNames, ","))

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
				d.logger.Warn("no job specifications are found for namespace [%s]", namespace.Name)
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

	deployIDs, err := d.processJobDeploymentResponses(stream)
	if err != nil {
		return err
	}

	d.logger.Info("> Polling deployment results:")

	var pollErrs error
	var wg sync.WaitGroup
	jobSpecService := pb.NewJobSpecificationServiceClient(conn.GetConnection())
	for _, deployID := range deployIDs {
		wg.Add(1)
		e := make(chan error)
		go func(deployID string, e chan error) {
			defer wg.Done()
			if err := PollJobDeployment(conn.GetContext(), d.logger, jobSpecService, deployTimeout, pollInterval, deployID); err != nil {
				e <- err
				return
			}
			e <- nil
		}(deployID, e)

		if err = <-e; err != nil {
			pollErrs = multierr.Append(pollErrs, err)
		}
	}
	wg.Wait()

	return pollErrs
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
	jobSpecReadWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs(), specio.WithJobSpecParentReading())
	if err != nil {
		return nil, err
	}

	jobSpecs, err := jobSpecReadWriter.ReadAll(namespace.Job.Path)
	if err != nil {
		return nil, err
	}

	jobSpecsProto := make([]*pb.JobSpecification, len(jobSpecs))
	for i, jobSpec := range jobSpecs {
		jobSpecsProto[i] = jobSpec.ToProto()
	}
	return &pb.DeployJobSpecificationRequest{
		Jobs:          jobSpecsProto,
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
			d.logger.Error("Deployment process took too long, timing out")
		}
		return nil, fmt.Errorf("deployement failed: %w", err)
	}
	return stream, nil
}

func (d *deployCommand) deployResources(
	conn *connectivity.Connectivity,
	selectedNamespaces []*config.Namespace,
) error {
	if d.ignoreResourceDeployment {
		d.logger.Warn("> Skipping resource deployment")
		return nil
	}
	d.logger.Info("> Deploying all resources")

	stream, err := d.getResourceStreamClient(conn)
	if err != nil {
		return err
	}

	var totalSpecsCount int
	for _, namespace := range selectedNamespaces {
		progressFn := func(totalCount int) {
			totalSpecsCount += totalCount
		}
		if err := d.sendNamespaceResourceRequest(stream, namespace, progressFn); err != nil {
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

	return d.processResourceDeploymentResponse(stream)
}

func (d *deployCommand) processResourceDeploymentResponse(
	stream pb.ResourceService_DeployResourceSpecificationClient,
) error {
	d.logger.Info("> Receiving responses:")

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		if logStatus := resp.GetLogStatus(); logStatus != nil {
			if d.verbose {
				logger.PrintLogStatusVerbose(d.logger, logStatus)
			} else {
				logger.PrintLogStatus(d.logger, logStatus)
			}
		}
	}
	return nil
}

func (d *deployCommand) sendNamespaceResourceRequest(
	stream pb.ResourceService_DeployResourceSpecificationClient,
	namespace *config.Namespace, progressFn func(totalCount int),
) error {
	datastoreSpecFs := resource.CreateDataStoreSpecFs(namespace)
	for storeName, repoFS := range datastoreSpecFs {
		d.logger.Info("> Deploying %s resources for namespace [%s]", storeName, namespace.Name)
		request, err := d.getResourceDeploymentRequest(namespace.Name, storeName, repoFS)
		if err != nil {
			if errors.Is(err, models.ErrNoResources) {
				d.logger.Warn("no resource specifications are found for namespace [%s]", namespace.Name)
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
	namespaceName, storeName string, repoFS afero.Fs,
) (*pb.DeployResourceSpecificationRequest, error) {
	resourceSpecReadWritter, err := specio.NewResourceSpecReadWriter(repoFS)
	if err != nil {
		return nil, err
	}

	resourceSpecs, err := resourceSpecReadWritter.ReadAll(".")
	if err != nil {
		return nil, err
	}

	resourceSpecsProto := make([]*pb.ResourceSpecification, len(resourceSpecs))
	for i, resourceSpec := range resourceSpecs {
		resourceSpecProto, err := resourceSpec.ToProto()
		if err != nil {
			return nil, err
		}
		resourceSpecsProto[i] = resourceSpecProto
	}

	return &pb.DeployResourceSpecificationRequest{
		Resources:     resourceSpecsProto,
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
			d.logger.Error("Deployment process took too long, timing out")
		}
		return nil, fmt.Errorf("deployement failed: %w", err)
	}
	return stream, nil
}

func (d *deployCommand) processJobDeploymentResponses(stream pb.JobSpecificationService_DeployJobSpecificationClient) ([]string, error) {
	deployIDMaps := map[string]bool{}
	d.logger.Info("> Receiving responses:")

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return []string{}, err
		}

		if logStatus := resp.GetLogStatus(); logStatus != nil {
			if d.verbose {
				logger.PrintLogStatusVerbose(d.logger, logStatus)
			} else {
				logger.PrintLogStatus(d.logger, logStatus)
			}
			continue
		}

		deploymentID := resp.GetDeploymentId()
		deployIDMaps[deploymentID] = true
		d.logger.Info("deployID %s successfully submitted\n", deploymentID)
	}

	deployIDs := []string{}
	for deployID := range deployIDMaps {
		deployIDs = append(deployIDs, deployID)
	}
	return deployIDs, nil
}

func PollJobDeployment(ctx context.Context, l log.Logger, jobSpecService pb.JobSpecificationServiceClient, deployTimeout, pollInterval time.Duration, deployID string) error {
	for keepPolling, timeout := true, time.After(deployTimeout); keepPolling; {
		resp, err := jobSpecService.GetDeployJobsStatus(ctx, &pb.GetDeployJobsStatusRequest{
			DeployId: deployID,
		})
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Error("Get deployment process took too long, timing out")
			}
			return fmt.Errorf("getting deployment status failed: %w", err)
		}

		switch resp.Status {
		case deploymentInProgress:
			l.Info("Deployment request for deployID %s is in progress...", deployID)
		case deploymentInQueue:
			l.Info("Deployment request for deployID %s is in queue...", deployID)
		case deploymentCancelled:
			l.Error("Deployment request for deployID %s is cancelled.", deployID)
			return errors.New("job deployment cancelled")
		case deploymentSucceed:
			l.Info("Success deploying %d jobs for deployID %s", resp.SuccessCount, deployID)
			return nil
		case deploymentFailed:
			if len(resp.Failures) > 0 {
				for _, failedJob := range resp.Failures {
					if failedJob.GetJobName() != "" {
						l.Error("Unable to deploy job %s: %s", failedJob.GetJobName(), failedJob.GetMessage())
					} else {
						l.Error("Job deployment failed: %s", failedJob.GetMessage())
					}
				}
			}
			if len(resp.UnknownDependencies) > 0 {
				l.Error("Unable to create sensors for below jobs:")
				for jobName, dependencies := range resp.UnknownDependencies {
					l.Error("- %s: invalid dependency name(s): %s.", jobName, dependencies)
				}
			}
			l.Error("Deployed %d/%d jobs.", resp.SuccessCount, resp.SuccessCount+resp.FailureCount)
			return errors.New("job deployment failed")
		}

		time.Sleep(pollInterval)

		select {
		case <-timeout:
			keepPolling = false
		default:
		}
	}
	return nil
}
