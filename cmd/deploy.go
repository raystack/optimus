package cmd

import (
	"context"
	"fmt"
	"io"
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
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	deploymentTimeout = time.Minute * 15
)

// deployCommand pushes current repo to optimus service
func deployCommand(l log.Logger, conf config.Optimus, jobSpecRepo JobSpecRepository,
	pluginRepo models.PluginRepository, datastoreRepo models.DatastoreRepo, datastoreSpecFs map[string]afero.Fs) *cli.Command {
	var (
		projectName     string
		namespace       string
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
	cmd.Flags().StringVarP(&projectName, "project", "p", conf.Project.Name, "Optimus project name")
	cmd.Flags().StringVarP(&namespace, "namespace", "n", conf.Namespace.Name, "Namespace of optimus project")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Print details related to deployment stages")
	cmd.Flags().BoolVar(&ignoreJobs, "ignore-jobs", false, "Ignore deployment of jobs")
	cmd.Flags().BoolVar(&ignoreResources, "ignore-resources", false, "Ignore deployment of resources")

	cmd.RunE = func(c *cli.Command, args []string) error {
		if projectName == "" || namespace == "" {
			return fmt.Errorf("project and namespace configurations are required")
		}

		l.Info(fmt.Sprintf("Deploying project: %s for namespace: %s at %s", projectName, namespace, conf.Host))
		start := time.Now()
		if jobSpecRepo == nil {
			// job repo not configured
			ignoreJobs = true
		}

		if err := postDeploymentRequest(l, projectName, namespace, jobSpecRepo, conf, pluginRepo, datastoreRepo,
			datastoreSpecFs, ignoreJobs, ignoreResources, verbose); err != nil {
			return err
		}
		l.Info(coloredSuccess("\nDeployment completed, took %s", time.Since(start).Round(time.Second)))
		return nil
	}

	return cmd
}

// postDeploymentRequest send a deployment request to service
func postDeploymentRequest(l log.Logger, projectName string, namespaceName string, jobSpecRepo JobSpecRepository,
	conf config.Optimus, pluginRepo models.PluginRepository, datastoreRepo models.DatastoreRepo, datastoreSpecFs map[string]afero.Fs,
	ignoreJobDeployment, ignoreResources bool, verbose bool) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, conf.Host); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Error(ErrServerNotReachable(conf.Host).Error())
		}
		return err
	}
	defer conn.Close()

	deployTimeoutCtx, deployCancel := context.WithTimeout(context.Background(), deploymentTimeout)
	defer deployCancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	adapt := v1handler.NewAdapter(pluginRepo, datastoreRepo)

	projectSpec := &pb.ProjectSpecification{
		Name:   projectName,
		Config: conf.Project.Config,
	}
	if err = registerProject(deployTimeoutCtx, l, runtime, projectSpec); err != nil {
		return err
	}

	namespaceSpec := &pb.NamespaceSpecification{
		Name:   namespaceName,
		Config: conf.Namespace.Config,
	}
	if err = registerNamespace(deployTimeoutCtx, l, runtime, projectSpec.Name, namespaceSpec); err != nil {
		return err
	}

	if !ignoreResources {
		// deploy datastore resources
		for storeName, repoFS := range datastoreSpecFs {
			l.Info(fmt.Sprintf("\n> Deploying resources for %s", storeName))
			ds, err := datastoreRepo.GetByName(storeName)
			if err != nil {
				return fmt.Errorf("unsupported datastore: %s\n", storeName)
			}
			resourceSpecRepo := local.NewResourceSpecRepository(repoFS, ds)
			resourceSpecs, err := resourceSpecRepo.GetAll(context.Background())
			if err == models.ErrNoResources {
				l.Info(coloredNotice("no resource specifications found"))
				continue
			}
			if err != nil {
				return errors.Wrap(err, "resourceSpecRepo.GetAll()")
			}

			// prepare specs
			adaptedSpecs := []*pb.ResourceSpecification{}
			for _, spec := range resourceSpecs {
				adapted, err := adapt.ToResourceProto(spec)
				if err != nil {
					return errors.Wrapf(err, "failed to serialize: %s", spec.Name)
				}
				adaptedSpecs = append(adaptedSpecs, adapted)
			}

			// send call
			respStream, err := runtime.DeployResourceSpecification(deployTimeoutCtx, &pb.DeployResourceSpecificationRequest{
				Resources:     adaptedSpecs,
				ProjectName:   projectSpec.Name,
				DatastoreName: storeName,
				NamespaceName: namespaceSpec.Name,
			})
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					l.Error(coloredError("Deployment process took too long, timing out"))
				}
				return errors.Wrapf(err, "deployement failed")
			}

			// track progress
			deployCounter := 0
			totalSpecs := len(adaptedSpecs)
			spinner := NewProgressBar()
			if !verbose {
				spinner.StartProgress(totalSpecs, "please wait")
			}
			for {
				resp, err := respStream.Recv()
				if err != nil {
					if err == io.EOF {
						break
					}
					return errors.Wrapf(err, "failed to receive deployment ack")
				}
				if resp.Ack {
					// ack for the resource spec
					if !resp.GetSuccess() {
						return errors.Errorf("unable to deploy resource: %s %s", resp.GetResourceName(), resp.GetMessage())
					}
					deployCounter++
					spinner.SetProgress(deployCounter)
					if verbose {
						l.Info(fmt.Sprintf("%d/%d. %s successfully deployed", deployCounter, totalSpecs, resp.GetResourceName()))
					}
				} else {
					if verbose {
						// ordinary progress event
						l.Info(fmt.Sprintf("info '%s': %s", resp.GetResourceName(), resp.GetMessage()))
					}
				}
			}
			spinner.Stop()
			l.Info(coloredSuccess("Successfully deployed %d/%d resources.", deployCounter, totalSpecs))
		}
	} else {
		l.Info("> Skipping resource deployment")
	}

	if !ignoreJobDeployment {
		// deploy job specifications
		l.Info("\n> Deploying jobs")
		jobSpecs, err := jobSpecRepo.GetAll()
		if err != nil {
			return err
		}

		var adaptedJobSpecs []*pb.JobSpecification
		for _, spec := range jobSpecs {
			adaptJob, err := adapt.ToJobProto(spec)
			if err != nil {
				return errors.Wrapf(err, "failed to serialize: %s", spec.Name)
			}
			adaptedJobSpecs = append(adaptedJobSpecs, adaptJob)
		}
		respStream, err := runtime.DeployJobSpecification(deployTimeoutCtx, &pb.DeployJobSpecificationRequest{
			Jobs:          adaptedJobSpecs,
			ProjectName:   projectSpec.Name,
			NamespaceName: namespaceSpec.Name,
		})
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Error(coloredError("Deployment process took too long, timing out"))
			}
			return errors.Wrapf(err, "deployement failed")
		}

		ackCounter := 0
		totalJobs := len(jobSpecs)
		spinner := NewProgressBar()
		if !verbose {
			spinner.StartProgress(totalJobs, "please wait")
			spinner.SetProgress(0)
		}
		var streamError error
		for {
			resp, err := respStream.Recv()
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
					return errors.Errorf("unable to deploy: %s %s", resp.GetJobName(), resp.GetMessage())
				}
				ackCounter++
				spinner.SetProgress(ackCounter)
				if verbose {
					l.Info(fmt.Sprintf("%d/%d. %s successfully deployed", ackCounter, totalJobs, resp.GetJobName()))
				}
			} else {
				if verbose {
					// ordinary progress event
					if resp.GetJobName() != "" {
						l.Info(fmt.Sprintf("info '%s': %s", resp.GetJobName(), resp.GetMessage()))
					} else {
						l.Info(fmt.Sprintf("info: %s", resp.GetMessage()))
					}
				}
			}
		}
		spinner.Stop()

		if streamError != nil {
			if ackCounter == totalJobs {
				// if we have uploaded all jobs successfully, further steps in pipeline
				// should not cause errors to fail and should end with warnings if any.
				l.Warn(coloredNotice("jobs deployed with warning"), "err", streamError)
			} else {
				return errors.Wrap(streamError, "failed to receive success deployment ack")
			}
		}
		l.Info(coloredSuccess("Successfully deployed %d/%d jobs.", ackCounter, totalJobs))
	} else {
		l.Info("> Skipping job deployment")
	}

	return nil
}

func registerProject(deployTimeoutCtx context.Context, l log.Logger, runtime pb.RuntimeServiceClient,
	projectSpec *pb.ProjectSpecification) (err error) {
	registerResponse, err := runtime.RegisterProject(deployTimeoutCtx, &pb.RegisterProjectRequest{
		Project: projectSpec,
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			l.Warn(coloredNotice("> Ignoring project config changes: %s", err.Error()))
			return nil
		}
		return errors.Wrap(err, "failed to update project configurations")
	} else if !registerResponse.Success {
		return fmt.Errorf("failed to update project configurations, %s", registerResponse.Message)
	}
	l.Info("\n> Updated project configuration")
	return nil
}

func registerNamespace(deployTimeoutCtx context.Context, l log.Logger, runtime pb.RuntimeServiceClient,
	projectName string, namespaceSpec *pb.NamespaceSpecification) (err error) {
	registerResponse, err := runtime.RegisterProjectNamespace(deployTimeoutCtx, &pb.RegisterProjectNamespaceRequest{
		ProjectName: projectName,
		Namespace:   namespaceSpec,
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			l.Warn(coloredNotice("> Ignoring namespace config changes: %s", err.Error()))
			return nil
		}
		return errors.Wrap(err, "failed to update namespace configurations")
	} else if !registerResponse.Success {
		return fmt.Errorf("failed to update namespace configurations, %s", registerResponse.Message)
	}
	l.Info("\n> Updated namespace configuration")
	return nil
}
