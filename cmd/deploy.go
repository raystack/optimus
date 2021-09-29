package cmd

import (
	"context"
	"fmt"
	"io"
	"time"

	v1handler "github.com/odpf/optimus/api/handler/v1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
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
func deployCommand(l log.Logger, conf config.Provider, jobSpecRepo JobSpecRepository,
	pluginRepo models.PluginRepository, datastoreRepo models.DatastoreRepo, datastoreSpecFs map[string]afero.Fs) *cli.Command {
	var projectName string
	var namespace string
	var ignoreJobs bool
	var ignoreResources bool

	cmd := &cli.Command{
		Use:   "deploy",
		Short: "Deploy current project to server",
	}
	cmd.Flags().StringVar(&projectName, "project", "", "project name of deployee")
	cmd.MarkFlagRequired("project")
	cmd.Flags().StringVar(&namespace, "namespace", "", "namespace of deployee")
	cmd.MarkFlagRequired("namespace")
	cmd.Flags().BoolVar(&ignoreJobs, "ignore-jobs", false, "ignore deployment of jobs")
	cmd.Flags().BoolVar(&ignoreResources, "ignore-resources", false, "ignore deployment of resources")

	cmd.RunE = func(c *cli.Command, args []string) error {
		l.Info(fmt.Sprintf("deploying project %s for namespace %s at %s\nplease wait...", projectName, namespace, conf.GetHost()))
		start := time.Now()
		if jobSpecRepo == nil {
			// job repo not configured
			ignoreJobs = true
		}

		if err := postDeploymentRequest(l, projectName, namespace, jobSpecRepo, conf, pluginRepo, datastoreRepo,
			datastoreSpecFs, ignoreJobs, ignoreResources); err != nil {
			return err
		}

		l.Info(fmt.Sprintf("deployment took %v", time.Since(start)))
		return nil
	}

	return cmd
}

// postDeploymentRequest send a deployment request to service
func postDeploymentRequest(l log.Logger, projectName string, namespace string, jobSpecRepo JobSpecRepository,
	conf config.Provider, pluginRepo models.PluginRepository, datastoreRepo models.DatastoreRepo, datastoreSpecFs map[string]afero.Fs,
	ignoreJobDeployment, ignoreResources bool) (err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, conf.GetHost()); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			l.Info("can't reach optimus service")
		}
		return err
	}
	defer conn.Close()

	deployTimeoutCtx, deployCancel := context.WithTimeout(context.Background(), deploymentTimeout)
	defer deployCancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	adapt := v1handler.NewAdapter(pluginRepo, datastoreRepo)

	// update project config if needed
	registerResponse, err := runtime.RegisterProject(deployTimeoutCtx, &pb.RegisterProjectRequest{
		Project: &pb.ProjectSpecification{
			Name:   projectName,
			Config: conf.GetProjectConfig().Global,
		},
		Namespace: &pb.NamespaceSpecification{
			Name:   namespace,
			Config: conf.GetProjectConfig().Local,
		},
	})
	if err != nil {
		return errors.Wrap(err, "failed to update project configurations")
	} else if !registerResponse.Success {
		return fmt.Errorf("failed to update project configurations, %s", registerResponse.Message)
	}
	l.Info("updated project configuration")

	if !ignoreResources {
		// deploy datastore resources
		for storeName, repoFS := range datastoreSpecFs {
			ds, err := datastoreRepo.GetByName(storeName)
			if err != nil {
				return fmt.Errorf("unsupported datastore: %s\n", storeName)
			}
			resourceSpecRepo := local.NewResourceSpecRepository(repoFS, ds)
			resourceSpecs, err := resourceSpecRepo.GetAll()
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
				ProjectName:   projectName,
				DatastoreName: storeName,
				Namespace:     namespace,
			})
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					l.Info("deployment process took too long, timing out")
				}
				return errors.Wrapf(err, "deployement failed")
			}

			// track progress
			deployCounter := 0
			totalSpecs := len(adaptedSpecs)
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
						return errors.Errorf("unable to deploy: %s %s", resp.GetResourceName(), resp.GetMessage())
					}
					deployCounter++
					l.Info(fmt.Sprintf("%d/%d. %s successfully deployed", deployCounter, totalSpecs, resp.GetResourceName()))
				} else {
					// ordinary progress event
					l.Info(fmt.Sprintf("info '%s': %s", resp.GetResourceName(), resp.GetMessage()))
				}
			}
		}
		l.Info("deployed resources")
	} else {
		l.Info("skipping resource deployment")
	}

	if !ignoreJobDeployment {
		// deploy job specifications
		l.Info("deploying jobs")
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
			Jobs:        adaptedJobSpecs,
			ProjectName: projectName,
			Namespace:   namespace,
		})
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				l.Info("deployment process took too long, timing out")
			}
			return errors.Wrapf(err, "deployement failed")
		}

		jobCounter := 0
		totalJobs := len(jobSpecs)
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
				jobCounter++
				l.Info(fmt.Sprintf("%d/%d. %s successfully deployed", jobCounter, totalJobs, resp.GetJobName()))
			} else {
				// ordinary progress event
				if resp.GetJobName() != "" {
					l.Info(fmt.Sprintf("info '%s': %s", resp.GetJobName(), resp.GetMessage()))
				} else {
					l.Info(fmt.Sprintf("info: %s", resp.GetMessage()))
				}
			}
		}

		if streamError != nil {
			if jobCounter == totalJobs {
				// if we have uploaded all jobs successfully, further steps in pipeline
				// should not cause errors to fail and should end with warnings if any.
				l.Warn(coloredNotice("jobs deployed with warning"), "err", streamError)
			} else {
				return errors.Wrapf(err, "failed to receive deployment ack")
			}
		}
		l.Info(fmt.Sprintf("successfully deployed %d/%d jobs", jobCounter, totalJobs))
	} else {
		l.Info("skipping job deployment")
	}

	l.Info("deployment completed")
	return nil
}
