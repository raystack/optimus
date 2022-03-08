package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/MakeNowJust/heredoc"
	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	cli "github.com/spf13/cobra"
)

const (
	deploymentTimeout = time.Minute * 15
)

// deployCommand pushes current repo to optimus service
func deployCommand(l log.Logger, conf config.Optimus, pluginRepo models.PluginRepository, dsRepo models.DatastoreRepo,
	datastoreSpecFs map[string]map[string]afero.Fs) *cli.Command {
	var (
		namespaces      []string
		ignoreJobs      bool
		ignoreResources bool
		verbose         bool
		cmd             = &cli.Command{
			Use:   "deploy",
			Short: "Deploy current optimus project to server",
			Long: heredoc.Doc(`Apply local changes to destination server which includes creating/updating/deleting
				jobs and creating/updating datastore resources`),
			Example: "optimus deploy [--ignore-resources|--ignore-jobs]",
			Annotations: map[string]string{
				"group:core": "true",
			},
		}
	)
	cmd.Flags().StringSliceVarP(&namespaces, "namespaces", "N", nil, "Selected namespaces of optimus project")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Print details related to deployment stages")
	cmd.Flags().BoolVar(&ignoreJobs, "ignore-jobs", false, "Ignore deployment of jobs")
	cmd.Flags().BoolVar(&ignoreResources, "ignore-resources", false, "Ignore deployment of resources")

	cmd.RunE = func(c *cli.Command, args []string) error {
		l.Info(fmt.Sprintf("Deploying project: %s to %s", conf.Project.Name, conf.Host))
		start := time.Now()

		if err := validateNamespaces(datastoreSpecFs, namespaces); err != nil {
			return err
		}
		err := postDeploymentRequest(l, conf, pluginRepo, dsRepo, datastoreSpecFs, namespaces, ignoreJobs, ignoreResources, verbose)
		if err != nil {
			return err
		}
		l.Info(coloredSuccess("\nDeployment completed, took %s", time.Since(start).Round(time.Second)))
		return nil
	}

	return cmd
}

// postDeploymentRequest send a deployment request to service
func postDeploymentRequest(l log.Logger, conf config.Optimus, pluginRepo models.PluginRepository,
	datastoreRepo models.DatastoreRepo, datastoreSpecFs map[string]map[string]afero.Fs, namespaceNames []string,
	ignoreJobDeployment, ignoreResources bool, verbose bool) error {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	conn, err := createConnection(dialTimeoutCtx, conf.Host)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(conf.Host).Error())
		}
		return err
	}
	defer conn.Close()

	deployTimeoutCtx, deployCancel := context.WithTimeout(context.Background(), deploymentTimeout)
	defer deployCancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	if err := registerProject(deployTimeoutCtx, runtime, l, conf); err != nil {
		return err
	}
	if err := registerAllNamespaces(deployTimeoutCtx, runtime, l, conf, namespaceNames); err != nil {
		return err
	}

	if !ignoreResources {
		if err := deployAllResources(deployTimeoutCtx,
			runtime, l,
			pluginRepo, datastoreRepo,
			datastoreSpecFs,
			conf.Project.Name, namespaceNames,
			verbose,
		); err != nil {
			return err
		}
	} else {
		l.Info("> Skipping resource deployment")
	}
	if !ignoreJobDeployment {
		if err := deployAllJobs(deployTimeoutCtx,
			runtime, l,
			conf, pluginRepo,
			datastoreRepo, namespaceNames,
			verbose,
		); err != nil {
			return err
		}
	} else {
		l.Info("> Skipping job deployment")
	}
	return nil
}

func deployAllJobs(deployTimeoutCtx context.Context,
	runtime pb.RuntimeServiceClient,
	l log.Logger, conf config.Optimus, pluginRepo models.PluginRepository,
	datastoreRepo models.DatastoreRepo,
	namespaceNames []string,
	verbose bool,
) error {
	ch := make(chan error, len(namespaceNames))
	defer close(ch)
	for _, namespaceName := range namespaceNames {
		go func(name string) {
			ch <- deployJob(deployTimeoutCtx,
				runtime,
				l, conf, pluginRepo,
				datastoreRepo,
				name,
				verbose,
			)
		}(namespaceName)
	}
	spinner := NewProgressBar()
	if !verbose {
		spinner.StartProgress(len(namespaceNames), "please wait")
	}
	var errMsg string
	for i := 0; i < len(namespaceNames); i++ {
		if err := <-ch; err != nil {
			errMsg += err.Error() + "\n"
		}
		spinner.SetProgress(i + 1)
	}
	spinner.Stop()
	if len(errMsg) > 0 {
		return errors.New(errMsg)
	}
	return nil
}

func deployJob(deployTimeoutCtx context.Context,
	runtime pb.RuntimeServiceClient,
	l log.Logger, conf config.Optimus, pluginRepo models.PluginRepository,
	datastoreRepo models.DatastoreRepo,
	namespaceName string,
	verbose bool,
) error {
	jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), conf.Namespaces[namespaceName].Job.Path)
	jobSpecRepo := local.NewJobSpecRepository(
		jobSpecFs,
		local.NewJobSpecAdapter(pluginRepo),
	)
	// deploy job specifications
	l.Info("\n> [%s] Deploying jobs", namespaceName)
	jobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return err
	}

	var adaptedJobSpecs []*pb.JobSpecification
	adapt := v1handler.NewAdapter(pluginRepo, datastoreRepo)
	for _, spec := range jobSpecs {
		adaptJob, err := adapt.ToJobProto(spec)
		if err != nil {
			return fmt.Errorf("[%s] failed to serialize: %s: %w", namespaceName, spec.Name, err)
		}
		adaptedJobSpecs = append(adaptedJobSpecs, adaptJob)
	}
	stream, err := runtime.DeployJobSpecification(deployTimeoutCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("[%s] Deployment process took too long, timing out", namespaceName))
		}
		return fmt.Errorf("[%s] deployement failed: %w", namespaceName, err)
	}
	if err := stream.Send(&pb.DeployJobSpecificationRequest{
		Jobs:          adaptedJobSpecs,
		ProjectName:   conf.Project.Name,
		NamespaceName: namespaceName,
	}); err != nil {
		return fmt.Errorf("[%s] deployment failed: %w", namespaceName, err)
	}

	ackCounter := 0
	totalJobs := len(jobSpecs)
	var streamError error
	for {
		resp, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			streamError = err
			break
		}
		if resp.Ack {
			// ack for the job spec
			if !resp.GetSuccess() {
				return fmt.Errorf("[%s] unable to deploy: %s %s: %w", namespaceName, resp.GetJobName(), resp.GetMessage(), err)
			}
			ackCounter++
			if verbose {
				l.Info(fmt.Sprintf("[%s] %d/%d. %s successfully deployed", namespaceName, ackCounter, totalJobs, resp.GetJobName()))
			}
		} else {
			if verbose {
				// ordinary progress event
				if resp.GetJobName() != "" {
					l.Info(fmt.Sprintf("[%s] info '%s': %s", namespaceName, resp.GetJobName(), resp.GetMessage()))
				} else {
					l.Info(fmt.Sprintf("[%s] info: %s", namespaceName, resp.GetMessage()))
				}
			}
		}
	}

	if streamError != nil {
		if ackCounter == totalJobs {
			// if we have uploaded all jobs successfully, further steps in pipeline
			// should not cause errors to fail and should end with warnings if any.
			l.Warn(coloredNotice("[%s] jobs deployed with warning", namespaceName), "err", streamError)
		} else {
			return fmt.Errorf("[%s] failed to receive success deployment ack: %w", namespaceName, streamError)
		}
	}
	l.Info(coloredSuccess("[%s] Successfully deployed %d/%d jobs.", namespaceName, ackCounter, totalJobs))
	return nil
}

func deployAllResources(deployTimeoutCtx context.Context,
	runtime pb.RuntimeServiceClient,
	l log.Logger, pluginRepo models.PluginRepository,
	datastoreRepo models.DatastoreRepo,
	datastoreSpecFs map[string]map[string]afero.Fs,
	projectName string, namespaceNames []string,
	verbose bool,
) error {
	ch := make(chan error, len(namespaceNames))
	defer close(ch)
	for _, namespaceName := range namespaceNames {
		go func(name string) {
			ch <- deployResource(deployTimeoutCtx,
				runtime,
				l, pluginRepo,
				datastoreRepo, datastoreSpecFs[name],
				projectName, name,
				verbose,
			)
		}(namespaceName)
	}
	spinner := NewProgressBar()
	if !verbose {
		spinner.StartProgress(len(namespaceNames), "please wait")
	}
	var errMsg string
	for i := 0; i < len(namespaceNames); i++ {
		if err := <-ch; err != nil {
			errMsg += err.Error() + "\n"
		}
		spinner.SetProgress(i + 1)
	}
	spinner.Stop()
	if len(errMsg) > 0 {
		return errors.New(errMsg)
	}
	return nil
}

func deployResource(deployTimeoutCtx context.Context,
	runtime pb.RuntimeServiceClient,
	l log.Logger, pluginRepo models.PluginRepository,
	datastoreRepo models.DatastoreRepo,
	datastoreSpecFs map[string]afero.Fs,
	projectName, namespaceName string,
	verbose bool,
) error {
	adapt := v1handler.NewAdapter(pluginRepo, datastoreRepo)
	// deploy datastore resources
	for storeName, repoFS := range datastoreSpecFs {
		l.Info(fmt.Sprintf("\n> [%s] Deploying resources for %s", namespaceName, storeName))
		ds, err := datastoreRepo.GetByName(storeName)
		if err != nil {
			return fmt.Errorf("[%s] unsupported datastore: %s", namespaceName, storeName)
		}
		resourceSpecRepo := local.NewResourceSpecRepository(repoFS, ds)
		resourceSpecs, err := resourceSpecRepo.GetAll(context.Background())
		if err == models.ErrNoResources {
			l.Info(coloredNotice("[%s] no resource specifications found", namespaceName))
			continue
		}
		if err != nil {
			return fmt.Errorf("[%s] resourceSpecRepo.GetAll(): %w", namespaceName, err)
		}

		// prepare specs
		adaptedSpecs := []*pb.ResourceSpecification{}
		for _, spec := range resourceSpecs {
			adapted, err := adapt.ToResourceProto(spec)
			if err != nil {
				return fmt.Errorf("[%s] failed to serialize: %s: %w", namespaceName, spec.Name, err)
			}
			adaptedSpecs = append(adaptedSpecs, adapted)
		}

		// send call
		stream, err := runtime.DeployResourceSpecification(deployTimeoutCtx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Error(coloredError("[%s] Deployment process took too long, timing out", namespaceName))
			}
			return fmt.Errorf("[%s] deployement failed: %w", namespaceName, err)
		}
		if err := stream.Send(&pb.DeployResourceSpecificationRequest{
			Resources:     adaptedSpecs,
			ProjectName:   projectName,
			DatastoreName: storeName,
			NamespaceName: namespaceName,
		}); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Error(coloredError("[%s] Deployment process took too long, timing out", namespaceName))
			}
			return fmt.Errorf("[%s] deployment failed: %w", namespaceName, err)
		}

		// track progress
		deployCounter := 0
		totalSpecs := len(adaptedSpecs)
		for {
			resp, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("[%s] failed to receive deployment ack: %w", namespaceName, err)
			}
			if resp.Ack {
				// ack for the resource spec
				if !resp.GetSuccess() {
					return fmt.Errorf("[%s] unable to deploy: %s %s", namespaceName, resp.GetResourceName(), resp.GetMessage())
				}
				deployCounter++
				if verbose {
					l.Info(fmt.Sprintf("[%s] %d/%d. %s successfully deployed", namespaceName, deployCounter, totalSpecs, resp.GetResourceName()))
				}
			} else {
				if verbose {
					// ordinary progress event
					l.Info(fmt.Sprintf("[%s] info '%s': %s", namespaceName, resp.GetResourceName(), resp.GetMessage()))
				}
			}
		}
		l.Info(coloredSuccess("[%s] Successfully deployed %d/%d resources.", namespaceName, deployCounter, totalSpecs))
	}
	return nil
}

func registerAllNamespaces(
	deployTimeoutCtx context.Context, runtime pb.RuntimeServiceClient,
	l log.Logger, conf config.Optimus, namespaceNames []string,
) error {
	ch := make(chan error, len(namespaceNames))
	defer close(ch)
	for i, namespaceName := range namespaceNames {
		go func(idx int, name string) {
			ch <- registerNamespace(deployTimeoutCtx, runtime, l, conf, name)
		}(i, namespaceName)
	}
	var errMsg string
	for i := 0; i < len(namespaceNames); i++ {
		if err := <-ch; err != nil {
			errMsg += err.Error() + "\n"
		}
	}
	if len(errMsg) > 0 {
		return errors.New(errMsg)
	}
	return nil
}

func registerNamespace(deployTimeoutCtx context.Context, runtime pb.RuntimeServiceClient,
	l log.Logger, conf config.Optimus, namespaceName string,
) error {
	namespace := conf.Namespaces[namespaceName]
	if namespace == nil {
		return fmt.Errorf("[%s] namespace is not found", namespaceName)
	}
	registerResponse, err := runtime.RegisterProjectNamespace(deployTimeoutCtx, &pb.RegisterProjectNamespaceRequest{
		ProjectName: conf.Project.Name,
		Namespace: &pb.NamespaceSpecification{
			Name:   namespaceName,
			Config: namespace.Config,
		},
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			l.Warn(coloredNotice("[%s] Ignoring namespace config changes: %s", namespaceName, err.Error()))
			return nil
		}
		return fmt.Errorf("failed to update namespace configurations: %w", err)
	} else if !registerResponse.Success {
		return fmt.Errorf("failed to update namespace configurations, %s", registerResponse.Message)
	}
	l.Info("\n> Updated namespace configuration")
	return nil
}

func registerProject(
	deployTimeoutCtx context.Context, runtime pb.RuntimeServiceClient,
	l log.Logger, conf config.Optimus,
) (err error) {
	projectSpec := &pb.ProjectSpecification{
		Name:   conf.Project.Name,
		Config: conf.Project.Config,
	}
	registerResponse, err := runtime.RegisterProject(deployTimeoutCtx, &pb.RegisterProjectRequest{
		Project: projectSpec,
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			l.Warn(coloredNotice("> Ignoring project config changes: %s", err.Error()))
			return nil
		}
		return fmt.Errorf("failed to update project configurations: %w", err)
	} else if !registerResponse.Success {
		return fmt.Errorf("failed to update project configurations, %s", registerResponse.Message)
	}
	l.Info("\n> Updated project configuration")
	return nil
}

func validateNamespaces(datastoreSpecFs map[string]map[string]afero.Fs, selectedNamespaceNames []string) error {
	var unknownNamespaceNames []string
	for _, namespaceName := range selectedNamespaceNames {
		if datastoreSpecFs[namespaceName] == nil {
			unknownNamespaceNames = append(unknownNamespaceNames, namespaceName)
		}
	}
	if len(unknownNamespaceNames) > 0 {
		return fmt.Errorf("[%s] namespaces are not found in config", strings.Join(unknownNamespaceNames, ", "))
	}
	return nil
}
