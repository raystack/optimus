package job

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	v1handler "github.com/odpf/optimus/api/handler/v1beta1"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
)

const validateTimeout = time.Minute * 5

type validateCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	verbose       bool
	namespaceName string
}

// NewValidateCommand initializes command for rendering job specification
func NewValidateCommand(logger log.Logger, clientConfig *config.ClientConfig) *cobra.Command {
	validate := &validateCommand{
		logger:       logger,
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use:     "render",
		Short:   "Apply template values in job specification to current 'render' directory",
		Long:    "Process optimus job specification based on macros/functions used.",
		Example: "optimus job render [<job_name>]",
		RunE:    validate.RunE,
	}
	cmd.Flags().BoolVarP(&validate.verbose, "verbose", "v", false, "Print details related to operation")
	cmd.Flags().StringVarP(&validate.namespaceName, "namespace", "n", validate.namespaceName, "Namespace of the resource within project")
	cmd.MarkFlagRequired("namespace")
	return cmd
}

func (v *validateCommand) RunE(cmd *cobra.Command, args []string) error {
	namespace, err := v.clientConfig.GetNamespaceByName(v.namespaceName)
	if err != nil {
		return err
	}

	pluginRepo := models.PluginRegistry
	jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), namespace.Job.Path)
	jobSpecRepo := local.NewJobSpecRepository(
		jobSpecFs,
		local.NewJobSpecAdapter(pluginRepo),
	)

	start := time.Now()
	projectName := v.clientConfig.Project.Name
	v.logger.Info(fmt.Sprintf("Validating job specifications for project: %s, namespace: %s", projectName, namespace.Name))
	jobSpecs, err := jobSpecRepo.GetAll()
	if err != nil {
		return fmt.Errorf("directory '%s': %w", namespace.Job.Path, err)
	}
	if err := v.validateJobSpecificationRequest(jobSpecs); err != nil {
		return err
	}
	v.logger.Info("Jobs validated successfully, took %s", time.Since(start).Round(time.Second))
	return nil
}

func (v *validateCommand) validateJobSpecificationRequest(jobSpecs []models.JobSpec) error {
	pluginRepo := models.PluginRegistry
	adapter := v1handler.NewAdapter(pluginRepo, models.DatastoreRegistry)

	conn, err := connectivity.NewConnectivity(v.clientConfig.Host, validateTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	adaptedJobSpecs := []*pb.JobSpecification{}
	for _, spec := range jobSpecs {
		adaptedJob := adapter.ToJobProto(spec)
		adaptedJobSpecs = append(adaptedJobSpecs, adaptedJob)
	}

	checkJobSpecRequest := &pb.CheckJobSpecificationsRequest{
		ProjectName:   v.clientConfig.Project.Name,
		Jobs:          adaptedJobSpecs,
		NamespaceName: v.namespaceName,
	}
	job := pb.NewJobSpecificationServiceClient(conn.GetConnection())
	respStream, err := job.CheckJobSpecifications(conn.GetContext(), checkJobSpecRequest)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			v.logger.Error("Validate process took too long, timing out")
		}
		return fmt.Errorf("validate request failed: %w", err)
	}
	return v.getCheckJobSpecificationsResponse(respStream, len(jobSpecs))
}

func (v *validateCommand) getCheckJobSpecificationsResponse(stream pb.JobSpecificationService_CheckJobSpecificationsClient, totalJobs int) error {
	ackCounter := 0
	failedCounter := 0

	spinner := progressbar.NewProgressBar()
	if !v.verbose {
		spinner.StartProgress(totalJobs, "validating jobs")
	}

	var validateErrors []string
	var streamError error
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			streamError = err
			break
		}
		if resp.Ack {
			// ack for the job spec
			if !resp.GetSuccess() {
				failedCounter++
				validateErrors = append(validateErrors, fmt.Sprintf("failed to validate: %s, %s\n", resp.GetJobName(), resp.GetMessage()))
			}
			ackCounter++
			if v.verbose {
				v.logger.Info(fmt.Sprintf("%d/%d. %s successfully checked", ackCounter, totalJobs, resp.GetJobName()))
			}
			spinner.SetProgress(ackCounter)
		} else if v.verbose {
			// ordinary progress event
			v.logger.Info(fmt.Sprintf("info '%s': %s", resp.GetJobName(), resp.GetMessage()))
		}
	}
	spinner.Stop()

	if len(validateErrors) > 0 {
		if v.verbose {
			for i, reqErr := range validateErrors {
				v.logger.Error(fmt.Sprintf("%d. %s", i+1, reqErr))
			}
		}
	} else if streamError != nil && ackCounter == totalJobs && failedCounter == 0 {
		// if we have uploaded all jobs successfully, further steps in pipeline
		// should not cause errors to fail and should end with warnings if any.
		v.logger.Warn("request ended with warning", "err", streamError)
		return nil
	}
	return streamError
}
