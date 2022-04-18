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
		namespaceNames  []string
		ignoreJobs      bool
		ignoreResources bool
		verbose         bool
		configFilePath  string
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
	cmd.Flags().StringSliceVarP(&namespaceNames, "namespace-names", "N", nil, "Selected namespaces of optimus project")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Print details related to deployment stages")
	cmd.Flags().BoolVar(&ignoreJobs, "ignore-jobs", false, "Ignore deployment of jobs")
	cmd.Flags().BoolVar(&ignoreResources, "ignore-resources", false, "Ignore deployment of resources")

	cmd.RunE = func(c *cli.Command, args []string) error {
		// TODO: find a way to load the config in one place
		conf, err := config.LoadClientConfig(configFilePath, cmd.Flags())
		if err != nil {
			return err
		}
		l := initClientLogger(conf.Log)

		// TODO: refactor initialize client deps
		pluginCleanFn, err := initializeClientPlugins(conf.Log.Level)
		defer pluginCleanFn()
		if err != nil {
			return err
		}

		l.Info(fmt.Sprintf("Deploying project: %s to %s", conf.Project.Name, conf.Host))
		start := time.Now()

		var selectedNamespaces []*config.Namespace
		if len(namespaceNames) > 0 {
			selectedNamespaces, err = conf.GetSelectedNamespaces(namespaceNames...)
			if err != nil {
				return err
			}
		} else {
			selectedNamespaces = conf.Namespaces
		}

		err = postDeploymentRequest(l, conf, selectedNamespaces, ignoreJobs, ignoreResources, verbose)
		if err != nil {
			return err
		}

		l.Info(coloredSuccess("\nDeployment completed, took %s", time.Since(start).Round(time.Second)))
		return nil
	}

	return cmd
}

// postDeploymentRequest send a deployment request to service
func postDeploymentRequest(l log.Logger, conf *config.ClientConfig,
	namespaces []*config.Namespace,
	ignoreJobDeployment, ignoreResources, verbose bool,
) error {
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

	if err := registerProject(l, conf.Host, conf.Project); err != nil {
		return err
	}
	if err := registerSelectedNamespaces(l, conf.Host, conf.Project.Name, namespaces...); err != nil {
		return err
	}

	pluginRepo := models.PluginRegistry
	datastoreRepo := models.DatastoreRegistry
	if !ignoreResources {
		resource := pb.NewResourceServiceClient(conn)
		if err := deployAllResources(deployTimeoutCtx,
			resource, l, conf,
			pluginRepo, datastoreRepo,
			namespaces,
			verbose,
		); err != nil {
			return err
		}
	} else {
		l.Info("> Skipping resource deployment")
	}
	if !ignoreJobDeployment {
		jobSpec := pb.NewJobSpecificationServiceClient(conn)
		if err := deployAllJobs(deployTimeoutCtx,
			jobSpec, l,
			conf, pluginRepo,
			datastoreRepo,
			namespaces,
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
	l log.Logger, conf *config.ClientConfig, pluginRepo models.PluginRepository,
	datastoreRepo models.DatastoreRepo,
	selectedNamespaces []*config.Namespace,
	verbose bool,
) error {
	// TODO fetch namespaces can be a separate function
	stream, err := jobSpecificationServiceClient.DeployJobSpecification(deployTimeoutCtx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(coloredError("Deployment process took too long, timing out"))
		}
		return errors.New("deployement failed")
	}
	var specFound bool
	var totalSpecsCount int
	for i, namespace := range selectedNamespaces {
		// TODO add a function to fetch jobspecs given namespace in protoformat

		// TODO  initialize the filesystem inside
		jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), namespace.Job.Path)
		jobSpecRepo := local.NewJobSpecRepository(
			jobSpecFs,
			local.NewJobSpecAdapter(pluginRepo),
		)
		// TODO Log once , new line can be logged outside
		if i == 0 {
			l.Info(fmt.Sprintf("\n> Deploying jobs for namespace [%s]", namespace.Name))
		} else {
			l.Info(fmt.Sprintf("> Deploying jobs for namespace [%s]", namespace.Name))
		}
		jobSpecs, err := jobSpecRepo.GetAll()
		if errors.Is(err, models.ErrNoJobs) {
			l.Info(coloredNotice("no job specifications are found for namespace [%s]", namespace.Name))
			continue
		}
		if err != nil {
			return fmt.Errorf("error getting job specs for namespace [%s]: %w", namespace.Name, err)
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
			NamespaceName: namespace.Name,
		}); err != nil {
			return fmt.Errorf("deployment for namespace [%s] failed: %w", namespace.Name, err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	if !specFound {
		l.Warn("no job specs are found from all the namespaces")
		return nil
	}

	l.Info("> Receiving responses:")
	// TODO spinner should be generic across all apis, we should avoid writing this logic for every api call
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

func deployAllResources(deployTimeoutCtx context.Context,
	resourceServiceClient pb.ResourceServiceClient,
	l log.Logger, conf *config.ClientConfig, pluginRepo models.PluginRepository,
	datastoreRepo models.DatastoreRepo,
	selectedNamespaces []*config.Namespace,
	verbose bool,
) error {
	datastoreSpecFs := getDatastoreSpecFs(conf.Namespaces)
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
	for _, namespace := range selectedNamespaces {
		adapt := v1handler.NewAdapter(pluginRepo, datastoreRepo)
		for storeName, repoFS := range datastoreSpecFs[namespace.Name] {
			l.Info(fmt.Sprintf("> Deploying %s resources for namespace [%s]", storeName, namespace.Name))
			ds, err := datastoreRepo.GetByName(storeName)
			if err != nil {
				return fmt.Errorf("unsupported datastore [%s] for namesapce [%s]", storeName, namespace.Name)
			}
			resourceSpecRepo := local.NewResourceSpecRepository(repoFS, ds)
			resourceSpecs, err := resourceSpecRepo.GetAll(deployTimeoutCtx)
			if errors.Is(err, models.ErrNoResources) {
				l.Info(coloredNotice("no resource specifications are found for namespace [%s]", namespace.Name))
				continue
			}
			if err != nil {
				return fmt.Errorf("error getting resource specs for namespace [%s]: %w", namespace.Name, err)
			}
			totalSpecsCount += len(resourceSpecs)

			// prepare specs
			adaptedSpecs := []*pb.ResourceSpecification{}
			for _, spec := range resourceSpecs {
				adapted, err := adapt.ToResourceProto(spec)
				if err != nil {
					return fmt.Errorf("failed to serialize [%s] for namespace [%s]: %w", spec.Name, namespace.Name, err)
				}
				adaptedSpecs = append(adaptedSpecs, adapted)
			}
			specFound = true
			if err := stream.Send(&pb.DeployResourceSpecificationRequest{
				Resources:     adaptedSpecs,
				ProjectName:   conf.Project.Name,
				DatastoreName: storeName,
				NamespaceName: namespace.Name,
			}); err != nil {
				return fmt.Errorf("deployment for namespace [%s] failed: %w", namespace.Name, err)
			}
		}
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	if !specFound {
		l.Warn("no resource specs are found from all the namespaces")
		return nil
	}

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
