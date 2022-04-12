package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	cli "github.com/spf13/cobra"

	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
)

const (
	deploymentTimeout = time.Minute * 15
)

// deployCommand pushes current repo to optimus service
func deployCommand() *cli.Command {
	var (
		selectedNamespaceNames []string
		ignoreJobs             bool
		ignoreResources        bool
		verbose                bool
		configFilePath         string
	)

	cmd := &cli.Command{
		Use:   "deploy",
		Short: "Deploy current optimus project to server",
		Long: heredoc.Doc(`Apply local changes to destination server which includes creating/updating/deleting
				jobs and creating/updating datastore resources`),
		Example: "optimus deploy [--ignore-resources|--ignore-jobs]",
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.Flags().StringVarP(&configFilePath, "config", "c", configFilePath, "File path for client configuration")
	cmd.Flags().StringSliceVarP(&selectedNamespaceNames, "namespace-names", "N", nil, "Selected namespaces of optimus project")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Print details related to deployment stages")
	cmd.Flags().BoolVar(&ignoreJobs, "ignore-jobs", false, "Ignore deployment of jobs")
	cmd.Flags().BoolVar(&ignoreResources, "ignore-resources", false, "Ignore deployment of resources")

	cmd.RunE = func(c *cli.Command, args []string) error {
		clientConfig, err := config.LoadClientConfig(configFilePath, cmd.Flags())
		if err != nil {
			return err
		}
		l := initClientLogger(clientConfig.Log)

		l.Info("Initializing client plugins")
		cleanupPlugin, err := initializeClientPlugins(clientConfig.Log.Level)
		if err != nil {
			return err
		}
		defer cleanupPlugin()

		l.Info(fmt.Sprintf("Registering project [%s] to [%s]", clientConfig.Project.Name, clientConfig.Host))
		if err := registerProject(l, clientConfig.Host, clientConfig.Project); err != nil {
			return err
		}

		l.Info("Validating namespaces")
		selectedNamespaces, err := clientConfig.GetSelectedNamespaces(selectedNamespaceNames...)
		if err != nil {
			return err
		}
		l.Info(fmt.Sprintf("Registering namespaces for [%s] to [%s]", clientConfig.Project.Name, clientConfig.Host))
		if err := registerSelectedNamespaces(l, clientConfig.Host, clientConfig.Project.Name, selectedNamespaces...); err != nil {
			return err
		}

		if ignoreJobs && ignoreResources {
			l.Info("No jobs and resources to be deployed")
			return nil
		}
		return postDeploymentRequest(l, clientConfig, selectedNamespaces, ignoreJobs, ignoreResources, verbose)
	}

	return cmd
}

// postDeploymentRequest send a deployment request to service
func postDeploymentRequest(
	l log.Logger, clientConfig *config.ClientConfig,
	selectedNamespaces []*config.Namespace,
	ignoreJobDeployment, ignoreResources, verbose bool,
) error {
	ctx, conn, closeConn, err := initClientConnection(l, clientConfig.Host, deploymentTimeout)
	if err != nil {
		return err
	}
	defer closeConn()

	if !ignoreResources {
		resourceServiceClient := pb.NewResourceServiceClient(conn)
		if err := deployAllResources(ctx,
			resourceServiceClient, l, clientConfig,
			selectedNamespaces,
			verbose,
		); err != nil {
			return err
		}
	} else {
		l.Info("> Skipping resource deployment")
	}

	if !ignoreJobDeployment {
		jobServiceClient := pb.NewJobSpecificationServiceClient(conn)
		if err := deployAllJobs(ctx,
			jobServiceClient, l, clientConfig,
			selectedNamespaces,
			verbose,
		); err != nil {
			return err
		}
	} else {
		l.Info("> Skipping job deployment")
	}
	return nil
}

func deployAllJobs(ctx context.Context,
	jobSpecificationServiceClient pb.JobSpecificationServiceClient,
	l log.Logger, clientConfig *config.ClientConfig,
	selectedNamespaces []*config.Namespace,
	verbose bool,
) error {
	stream, err := getJobStreamClient(ctx, l, jobSpecificationServiceClient)
	if err != nil {
		return err
	}

	var totalSpecsCount int
	for _, namespace := range selectedNamespaces {
		progressFn := func(totalCount int) {
			totalSpecsCount += totalCount
		}
		if err := sendNamespaceJobDeploymentRequest(
			stream,
			clientConfig.Project.Name, namespace,
			progressFn,
		); err != nil {
			if errors.Is(err, models.ErrNoJobs) {
				l.Info("no job specifications are found for namespace [%s]", namespace.Name)
				continue
			}
			return fmt.Errorf("error getting job specs for namespace [%s]: %w", namespace.Name, err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}

	if totalSpecsCount == 0 {
		l.Warn("no job specs are found from all the namespaces")
		return nil
	}
	return getJobDeploymentResponse(l, stream, totalSpecsCount, verbose)
}

func getJobDeploymentResponse(
	l log.Logger,
	stream pb.JobSpecificationService_DeployJobSpecificationClient,
	totalSpecsCount int, verbose bool,
) error {
	l.Info("> Receiving responses:")
	var counter int
	spinner := NewProgressBar()
	if !verbose {
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
				l.Error(resp.GetMessage())
			}
			if resp.GetJobName() != "" {
				counter++
				spinner.SetProgress(counter)
				if verbose {
					l.Info(fmt.Sprintf("[%d/%d] %s successfully deployed", counter, totalSpecsCount, resp.GetJobName()))
				}
			} else if verbose {
				l.Info(resp.Message)
			}
		}
	}
	spinner.Stop()
	return nil
}

func sendNamespaceJobDeploymentRequest(
	stream pb.JobSpecificationService_DeployJobSpecificationClient,
	projectName string, namespace *config.Namespace,
	progressFn func(totalCount int),
) error {
	request, err := getJobDeploymentRequest(projectName, namespace)
	if err != nil {
		return err
	}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("deployment for namespace [%s] failed: %w", namespace.Name, err)
	}
	progressFn(len(request.GetJobs()))
	return nil
}

func getJobDeploymentRequest(projectName string, namespace *config.Namespace) (*pb.DeployJobSpecificationRequest, error) {
	pluginRepo := models.PluginRegistry
	datastoreRepo := models.DatastoreRegistry
	adapter := v1handler.NewAdapter(pluginRepo, datastoreRepo)

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
		adaptedJobSpecs[i] = adapter.ToJobProto(spec)
	}
	return &pb.DeployJobSpecificationRequest{
		Jobs:          adaptedJobSpecs,
		ProjectName:   projectName,
		NamespaceName: namespace.Name,
	}, nil
}

func getJobStreamClient(
	ctx context.Context, l log.Logger,
	jobSpecificationServiceClient pb.JobSpecificationServiceClient,
) (pb.JobSpecificationService_DeployJobSpecificationClient, error) {
	stream, err := jobSpecificationServiceClient.DeployJobSpecification(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Deployment process took too long, timing out"))
		}
		return nil, fmt.Errorf("deployement failed: %w", err)
	}
	return stream, nil
}

func deployAllResources(
	ctx context.Context,
	resourceServiceClient pb.ResourceServiceClient,
	l log.Logger, clientConfig *config.ClientConfig,
	selectedNamespaces []*config.Namespace,
	verbose bool,
) error {
	stream, err := getResourceStreamClient(ctx, l, resourceServiceClient)
	if err != nil {
		return err
	}

	var totalSpecsCount int
	for _, namespace := range selectedNamespaces {
		progressFn := func(totalCount int) {
			totalSpecsCount += totalCount
		}
		if err := sendNamespaceResourceDeploymentRequest(
			ctx, l, stream,
			clientConfig.Project.Name, namespace,
			progressFn,
		); err != nil {
			return err
		}
	}

	if err := stream.CloseSend(); err != nil {
		return err
	}

	if totalSpecsCount == 0 {
		l.Warn("no resource specs are found from all the namespaces")
		return nil
	}
	return getResourceDeploymentResponse(l, stream, totalSpecsCount, verbose)
}

func getResourceDeploymentResponse(
	l log.Logger,
	stream pb.ResourceService_DeployResourceSpecificationClient,
	totalSpecsCount int, verbose bool,
) error {
	l.Info("> Receiving responses:")
	var counter int
	spinner := NewProgressBar()
	if !verbose {
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
				l.Error(resp.GetMessage())
			}
			if resp.GetResourceName() != "" {
				counter++
				spinner.SetProgress(counter)
				if verbose {
					l.Info(fmt.Sprintf("[%d/%d] %s successfully deployed", counter, totalSpecsCount, resp.GetResourceName()))
				}
			} else if verbose {
				l.Info(resp.Message)
			}
		}
	}
	spinner.Stop()
	return nil
}

func sendNamespaceResourceDeploymentRequest(
	ctx context.Context, l log.Logger, stream pb.ResourceService_DeployResourceSpecificationClient,
	projectName string, namespace *config.Namespace,
	progressFn func(totalCount int),
) error {
	datastoreSpecFs := createDataStoreSpecFs(namespace)
	for storeName, repoFS := range datastoreSpecFs {
		l.Info(fmt.Sprintf("> Deploying %s resources for namespace [%s]", storeName, namespace.Name))
		request, err := getResourceDeploymentRequest(ctx, projectName, namespace.Name, storeName, repoFS)
		if err != nil {
			if errors.Is(err, models.ErrNoResources) {
				l.Info("no resource specifications are found for namespace [%s]", namespace.Name)
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

func getResourceDeploymentRequest(
	ctx context.Context,
	projectName, namespaceName, storeName string,
	repoFS afero.Fs,
) (*pb.DeployResourceSpecificationRequest, error) {
	pluginRepo := models.PluginRegistry
	datastoreRepo := models.DatastoreRegistry
	adapter := v1handler.NewAdapter(pluginRepo, datastoreRepo)

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
		adapted, err := adapter.ToResourceProto(spec)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize [%s] for namespace [%s]: %w", spec.Name, namespaceName, err)
		}
		adaptedSpecs[i] = adapted
	}
	return &pb.DeployResourceSpecificationRequest{
		Resources:     adaptedSpecs,
		ProjectName:   projectName,
		DatastoreName: storeName,
		NamespaceName: namespaceName,
	}, nil
}

func getResourceStreamClient(
	ctx context.Context, l log.Logger,
	resourceServiceClient pb.ResourceServiceClient,
) (pb.ResourceService_DeployResourceSpecificationClient, error) {
	stream, err := resourceServiceClient.DeployResourceSpecification(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Deployment process took too long, timing out"))
		}
		return nil, fmt.Errorf("deployement failed: %w", err)
	}
	return stream, nil
}
