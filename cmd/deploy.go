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
	ignoreJobDeployment, ignoreResources, verbose bool) error {
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

	project := pb.NewProjectServiceClient(conn)
	namespace := pb.NewNamespaceServiceClient(conn)
	resource := pb.NewResourceServiceClient(conn)
	jobSpec := pb.NewJobSpecificationServiceClient(conn)

	if err := registerProject(deployTimeoutCtx, project, l, conf); err != nil {
		return err
	}
	if err := registerAllNamespaces(deployTimeoutCtx, namespace, l, conf, namespaceNames); err != nil {
		return err
	}

	if !ignoreResources {
		if err := deployAllResources(deployTimeoutCtx,
			resource, l, conf,
			pluginRepo, datastoreRepo,
			datastoreSpecFs,
			namespaceNames,
			verbose,
		); err != nil {
			return err
		}
	} else {
		l.Info("> Skipping resource deployment")
	}
	if !ignoreJobDeployment {
		if err := deployAllJobs(deployTimeoutCtx,
			jobSpec, l,
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
	jobSpecificationServiceClient pb.JobSpecificationServiceClient,
	l log.Logger, conf config.Optimus, pluginRepo models.PluginRepository,
	datastoreRepo models.DatastoreRepo,
	namespaceNames []string,
	verbose bool,
) error {
	// TODO fetch namespaces can be a separate function
	var selectedNamespaceNames []string
	if len(namespaceNames) > 0 {
		selectedNamespaceNames = namespaceNames
	} else {
		for _, namespace := range conf.Namespaces {
			selectedNamespaceNames = append(selectedNamespaceNames, namespace.Name)
		}
	}
	if len(selectedNamespaceNames) == 0 {
		return errors.New("no namespace is found to deploy")
	}

	stream, err := jobSpecificationServiceClient.DeployJobSpecification(deployTimeoutCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Deployment process took too long, timing out"))
		}
		return errors.New("deployement failed")
	}
	var specFound bool
	var totalSpecsCount int
	for i, namespaceName := range selectedNamespaceNames {
		// TODO add a function to fetch jobspecs given namespace in protoformat
		//TODO this check i believe is not necessary
		namespace, err := conf.GetNamespaceByName(namespaceName)
		if err != nil {
			return err
		}
		// TODO  initialize the filesystem inside
		jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), namespace.Job.Path)
		jobSpecRepo := local.NewJobSpecRepository(
			jobSpecFs,
			local.NewJobSpecAdapter(pluginRepo),
		)
		// TODO Log once , new line can be logged outside
		if i == 0 {
			l.Info(fmt.Sprintf("\n> Deploying jobs for namespace [%s]", namespaceName))
		} else {
			l.Info(fmt.Sprintf("> Deploying jobs for namespace [%s]", namespaceName))
		}
		jobSpecs, err := jobSpecRepo.GetAll()
		if err != nil {
			return err
		}
		if len(jobSpecs) == 0 {
			l.Warn("skipping deployment for namespace [%s] as job spec is empty", namespaceName)
			continue
		}
		totalSpecsCount += len(jobSpecs)

		// TODO rename to JobSpecsInProto
		var adaptedJobSpecs []*pb.JobSpecification
		adapt := v1handler.NewAdapter(pluginRepo, datastoreRepo)
		for _, spec := range jobSpecs {
			adaptJob := adapt.ToJobProto(spec)
			adaptedJobSpecs = append(adaptedJobSpecs, adaptJob)
		}
		specFound = true
		if err := stream.Send(&pb.DeployJobSpecificationRequest{
			Jobs:          adaptedJobSpecs,
			ProjectName:   conf.Project.Name,
			NamespaceName: namespaceName,
		}); err != nil {
			return fmt.Errorf("deployment for namespace [%s] failed: %w", namespaceName, err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	if !specFound {
		return nil
	}

	l.Info("> Receiving responses:")
	// TODO spinner should be generic across all apis, we should avoid writing this logic for every api call
	var counter int
	var streamErrs []error
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
				streamErrs = append(streamErrs, errors.New(resp.GetMessage()))
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
	if len(streamErrs) > 0 {
		for _, e := range streamErrs {
			l.Error(e.Error())
		}
		return errors.New("one or more errors are encountered during job deployment")
	}
	return nil
}

func deployAllResources(deployTimeoutCtx context.Context,
	resourceServiceClient pb.ResourceServiceClient,
	l log.Logger, conf config.Optimus, pluginRepo models.PluginRepository,
	datastoreRepo models.DatastoreRepo,
	datastoreSpecFs map[string]map[string]afero.Fs,
	namespaceNames []string,
	verbose bool,
) error {
	var selectedNamespaceNames []string
	if len(namespaceNames) > 0 {
		selectedNamespaceNames = namespaceNames
	} else {
		for _, namespace := range conf.Namespaces {
			selectedNamespaceNames = append(selectedNamespaceNames, namespace.Name)
		}
	}
	if len(selectedNamespaceNames) == 0 {
		return errors.New("no namespace is found to deploy")
	}

	// send call
	stream, err := resourceServiceClient.DeployResourceSpecification(deployTimeoutCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Deployment process took too long, timing out"))
		}
		return fmt.Errorf("deployement failed: %w", err)
	}
	var specFound bool
	var totalSpecsCount int
	for _, namespaceName := range selectedNamespaceNames {
		adapt := v1handler.NewAdapter(pluginRepo, datastoreRepo)
		for storeName, repoFS := range datastoreSpecFs[namespaceName] {
			l.Info(fmt.Sprintf("> Deploying %s resources for namespace [%s]", storeName, namespaceName))
			ds, err := datastoreRepo.GetByName(storeName)
			if err != nil {
				return fmt.Errorf("unsupported datastore [%s] for namesapce [%s]", storeName, namespaceName)
			}
			resourceSpecRepo := local.NewResourceSpecRepository(repoFS, ds)
			resourceSpecs, err := resourceSpecRepo.GetAll(context.Background())
			if errors.Is(err, models.ErrNoResources) {
				l.Info(coloredNotice("no resource specifications are found for namespace [%s]", namespaceName))
				continue
			}
			if err != nil {
				return fmt.Errorf("error getting specs for namespace [%s]: %w", namespaceName, err)
			}
			totalSpecsCount += len(resourceSpecs)

			// prepare specs
			adaptedSpecs := []*pb.ResourceSpecification{}
			for _, spec := range resourceSpecs {
				adapted, err := adapt.ToResourceProto(spec)
				if err != nil {
					return fmt.Errorf("failed to serialize [%s] for namespace [%s]: %w", spec.Name, namespaceName, err)
				}
				adaptedSpecs = append(adaptedSpecs, adapted)
			}
			specFound = true
			if err := stream.Send(&pb.DeployResourceSpecificationRequest{
				Resources:     adaptedSpecs,
				ProjectName:   conf.Project.Name,
				DatastoreName: storeName,
				NamespaceName: namespaceName,
			}); err != nil {
				return fmt.Errorf("deployment for namespace [%s] failed: %w", namespaceName, err)
			}
		}
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	if !specFound {
		return nil
	}

	l.Info("> Receiving responses:")
	var counter int
	var streamErrs []error
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
				streamErrs = append(streamErrs, errors.New(resp.GetMessage()))
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
	if len(streamErrs) > 0 {
		for _, e := range streamErrs {
			l.Error(e.Error())
		}
		return errors.New("one or more errors are encountered during resource deployment")
	}
	return nil
}

func registerAllNamespaces(
	deployTimeoutCtx context.Context, namespaceServiceClient pb.NamespaceServiceClient,
	l log.Logger, conf config.Optimus, namespaceNames []string,
) error {
	var selectedNamespaceNames []string
	if len(namespaceNames) > 0 {
		selectedNamespaceNames = namespaceNames
	} else {
		for _, namespace := range conf.Namespaces {
			selectedNamespaceNames = append(selectedNamespaceNames, namespace.Name)
		}
	}

	ch := make(chan error, len(selectedNamespaceNames))
	defer close(ch)
	for _, namespaceName := range selectedNamespaceNames {
		go func(name string) {
			ch <- registerNamespace(deployTimeoutCtx, namespaceServiceClient, l, conf, name)
		}(namespaceName)
	}
	var errMsg string
	for i := 0; i < len(selectedNamespaceNames); i++ {
		if err := <-ch; err != nil {
			errMsg += err.Error() + "\n"
		}
	}
	if len(errMsg) > 0 {
		return errors.New(errMsg)
	}
	return nil
}

func registerNamespace(deployTimeoutCtx context.Context, namespaceServiceClient pb.NamespaceServiceClient,
	l log.Logger, conf config.Optimus, namespaceName string,
) error {
	namespace, err := conf.GetNamespaceByName(namespaceName)
	if err != nil {
		return err
	}
	registerResponse, err := namespaceServiceClient.RegisterProjectNamespace(deployTimeoutCtx, &pb.RegisterProjectNamespaceRequest{
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
	deployTimeoutCtx context.Context, projectServiceClient pb.ProjectServiceClient,
	l log.Logger, conf config.Optimus,
) (err error) {
	projectSpec := &pb.ProjectSpecification{
		Name:   conf.Project.Name,
		Config: conf.Project.Config,
	}
	registerResponse, err := projectServiceClient.RegisterProject(deployTimeoutCtx, &pb.RegisterProjectRequest{
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
		return fmt.Errorf("namespaces [%s] are not found", strings.Join(unknownNamespaceNames, ", "))
	}
	return nil
}
