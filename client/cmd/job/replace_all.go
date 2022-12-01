package job

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/client/cmd/internal/connectivity"
	"github.com/odpf/optimus/client/cmd/internal/logger"
	"github.com/odpf/optimus/client/local/specio"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	pb "github.com/odpf/optimus/protos/odpf/optimus/core/v1beta1"
)

const (
	replaceAllTimeout = time.Minute * 30
)

type replaceAllCommand struct {
	logger       log.Logger
	clientConfig *config.ClientConfig

	selectedNamespaceNames []string
	verbose                bool
	configFilePath         string
}

// NewReplaceAllCommand initializes command for ReplaceAll
func NewReplaceAllCommand() *cobra.Command {
	replaceAll := &replaceAllCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:   "replace-all",
		Short: "Replace all current optimus project to server",
		Long: heredoc.Doc(`Apply local changes to destination server which includes creating/updating/deleting
				jobs`),
		Example: "optimus job replace-all [--ignore-resources|--ignore-jobs]",
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE:    replaceAll.RunE,
		PreRunE: replaceAll.PreRunE,
	}
	cmd.Flags().StringVarP(&replaceAll.configFilePath, "config", "c", replaceAll.configFilePath, "File path for client configuration")
	cmd.Flags().StringSliceVarP(&replaceAll.selectedNamespaceNames, "namespace-names", "N", nil, "Selected namespaces of optimus project")
	cmd.Flags().BoolVarP(&replaceAll.verbose, "verbose", "v", false, "Print details related to replace-all stages")
	return cmd
}

func (r *replaceAllCommand) PreRunE(_ *cobra.Command, _ []string) error {
	var err error
	r.clientConfig, err = config.LoadClientConfig(r.configFilePath)
	if err != nil {
		return err
	}
	return nil
}

func (r *replaceAllCommand) RunE(_ *cobra.Command, _ []string) error {
	r.logger.Info("Validating namespaces")
	selectedNamespaces, err := r.clientConfig.GetSelectedNamespaces(r.selectedNamespaceNames...)
	if err != nil {
		return err
	}
	if len(selectedNamespaces) == 0 {
		selectedNamespaces = r.clientConfig.Namespaces
	}
	r.logger.Info("validation finished!\n")

	return r.replaceAll(selectedNamespaces)
}

func (r *replaceAllCommand) replaceAll(selectedNamespaces []*config.Namespace) error {
	conn, err := connectivity.NewConnectivity(r.clientConfig.Host, replaceAllTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := r.replaceAllJobs(conn, selectedNamespaces); err != nil {
		return err
	}
	r.logger.Info("> replace all job specifications finished!\n")

	return nil
}

func (r *replaceAllCommand) replaceAllJobs(conn *connectivity.Connectivity, selectedNamespaces []*config.Namespace) error {
	namespaceNames := []string{}
	for _, namespace := range selectedNamespaces {
		namespaceNames = append(namespaceNames, namespace.Name)
	}
	r.logger.Info("> Replacing all jobs for namespaces [%s]", strings.Join(namespaceNames, ","))

	stream, err := r.getJobStreamClient(conn)
	if err != nil {
		return err
	}

	var totalSpecsCount int
	for _, namespace := range selectedNamespaces {
		progressFn := func(totalCount int) {
			totalSpecsCount += totalCount
		}
		if err := r.sendNamespaceJobRequest(stream, namespace, progressFn); err != nil {
			if errors.Is(err, models.ErrNoJobs) {
				r.logger.Warn("no job specifications are found for namespace [%s]", namespace.Name)
				continue
			}
			return fmt.Errorf("error getting job specs for namespace [%s]: %w", namespace.Name, err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}

	if totalSpecsCount == 0 {
		r.logger.Warn("no job specs are found from all the namespaces")
		return nil
	}

	return r.processJobReplaceAllResponses(stream)
}

func (r *replaceAllCommand) sendNamespaceJobRequest(
	stream pb.JobSpecificationService_ReplaceAllJobSpecificationsClient,
	namespace *config.Namespace,
	progressFn func(totalCount int),
) error {
	request, err := r.getReplaceAllRequest(r.clientConfig.Project.Name, namespace)
	if err != nil {
		return err
	}
	if err := stream.Send(request); err != nil {
		return fmt.Errorf("replacing jobs in namespace [%s] failed: %w", namespace.Name, err)
	}
	progressFn(len(request.GetJobs()))
	return nil
}

func (*replaceAllCommand) getReplaceAllRequest(projectName string, namespace *config.Namespace) (*pb.ReplaceAllJobSpecificationsRequest, error) {
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
	return &pb.ReplaceAllJobSpecificationsRequest{
		Jobs:          jobSpecsProto,
		ProjectName:   projectName,
		NamespaceName: namespace.Name,
	}, nil
}

func (r *replaceAllCommand) getJobStreamClient(
	conn *connectivity.Connectivity,
) (pb.JobSpecificationService_ReplaceAllJobSpecificationsClient, error) {
	client := pb.NewJobSpecificationServiceClient(conn.GetConnection())
	stream, err := client.ReplaceAllJobSpecifications(conn.GetContext())
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			r.logger.Error("Replace job specifications process took too long, timing out")
		}
		return nil, fmt.Errorf("replace job specifications failed: %w", err)
	}
	return stream, nil
}

func (r *replaceAllCommand) processJobReplaceAllResponses(stream pb.JobSpecificationService_ReplaceAllJobSpecificationsClient) error {
	r.logger.Info("> Receiving responses:")

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		if logStatus := resp.GetLogStatus(); logStatus != nil {
			if r.verbose {
				logger.PrintLogStatusVerbose(r.logger, logStatus)
			} else {
				logger.PrintLogStatus(r.logger, logStatus)
			}
			continue
		}
	}

	return nil
}
