package resource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/local/model"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	uploadAllTimeout = time.Minute * 60
)

type uploadAllCommand struct {
	logger     log.Logger
	connection connection.Connection

	clientConfig *config.ClientConfig

	selectedNamespaceNames []string
	verbose                bool
	configFilePath         string

	batchSize int
}

// NewUploadAllCommand initializes command for uploading all resources
func NewUploadAllCommand() *cobra.Command {
	uploadAll := &uploadAllCommand{
		logger: logger.NewClientLogger(),
	}

	cmd := &cobra.Command{
		Use:     "upload-all",
		Short:   "Upload all current optimus resources to server",
		Long:    heredoc.Doc(`Apply local changes to destination server which includes creating/updating resources`),
		Example: "optimus resource upload-all [--verbose | -b 1000]",
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE:    uploadAll.RunE,
		PreRunE: uploadAll.PreRunE,
	}
	cmd.Flags().StringVarP(&uploadAll.configFilePath, "config", "c", uploadAll.configFilePath, "File path for client configuration")
	cmd.Flags().StringSliceVarP(&uploadAll.selectedNamespaceNames, "namespace-names", "N", nil, "Selected namespaces of optimus project")
	cmd.Flags().BoolVarP(&uploadAll.verbose, "verbose", "v", false, "Print details related to upload-all stages")
	cmd.Flags().IntVarP(&uploadAll.batchSize, "batch-size", "b", 0, "Number of resources to upload in a batch")
	return cmd
}

func (u *uploadAllCommand) PreRunE(_ *cobra.Command, _ []string) error {
	var err error
	u.clientConfig, err = config.LoadClientConfig(u.configFilePath)
	if err != nil {
		return err
	}

	u.connection = connection.New(u.logger, u.clientConfig)
	return nil
}

func (u *uploadAllCommand) RunE(_ *cobra.Command, _ []string) error {
	u.logger.Info("> Validating namespaces")
	selectedNamespaces, err := u.clientConfig.GetSelectedNamespaces(u.selectedNamespaceNames...)
	if err != nil {
		return err
	}
	if len(selectedNamespaces) == 0 {
		selectedNamespaces = u.clientConfig.Namespaces
	}
	u.logger.Info("namespace validation finished!\n")

	return u.uploadAll(selectedNamespaces)
}

func (u *uploadAllCommand) uploadAll(selectedNamespaces []*config.Namespace) error {
	conn, err := u.connection.Create(u.clientConfig.Host)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancelFunc := context.WithTimeout(context.Background(), uploadAllTimeout)
	defer cancelFunc()

	if err := u.uploadAllResources(ctx, conn, selectedNamespaces); err != nil {
		return err
	}
	u.logger.Info("finished uploading resource specifications to server!\n")

	return nil
}

func (u *uploadAllCommand) uploadAllResources(ctx context.Context, conn *grpc.ClientConn, selectedNamespaces []*config.Namespace) error {
	var namespaceNames []string
	for _, namespace := range selectedNamespaces {
		namespaceNames = append(namespaceNames, namespace.Name)
	}

	u.logger.Info("> Uploading all resources for namespaces [%s]", strings.Join(namespaceNames, ","))

	stream, err := u.getResourceStreamClient(ctx, conn)
	if err != nil {
		return err
	}

	var totalSpecsCount int
	for _, namespace := range selectedNamespaces {
		progressFn := func(totalCount int) {
			totalSpecsCount += totalCount
		}
		if err := u.sendNamespaceResourceRequest(stream, namespace, progressFn); err != nil {
			return err
		}
	}

	if err := stream.CloseSend(); err != nil {
		return err
	}

	if totalSpecsCount == 0 {
		u.logger.Warn("no resource specs are found from all the namespaces")
		return nil
	}

	return u.processResourceDeploymentResponse(stream)
}

func (u *uploadAllCommand) sendNamespaceResourceRequest(stream pb.ResourceService_DeployResourceSpecificationClient,
	namespace *config.Namespace, progressFn func(totalCount int),
) error {
	datastoreSpecFs := CreateDataStoreSpecFs(namespace)
	for storeName, repoFS := range datastoreSpecFs {
		u.logger.Info("> Deploying %s resources for namespace [%s]", storeName, namespace.Name)

		resources, err := readResourceSpecs(repoFS)
		if err != nil {
			return fmt.Errorf("error getting resource specs for namespace [%s]: %w", namespace.Name, err)
		}

		resLength := len(resources)
		size := resLength
		if u.batchSize > 0 {
			size = u.batchSize
		}

		var errorReturned bool
		for i := 0; i < resLength; i += size {
			endIndex := i + size
			if resLength < endIndex {
				endIndex = resLength
			}

			request, err := u.getResourceDeploymentRequest(namespace.Name, storeName, resources[i:endIndex])
			if err != nil {
				u.logger.Error("Unable to get resource request, err: %s", err)
				continue
			}

			if err = stream.Send(request); err != nil {
				errorReturned = true
				u.logger.Error("Error: %s", err)
			}
			progressFn(len(request.GetResources()))
		}
		if errorReturned {
			return fmt.Errorf("resource upload for namespace [%s] failed", namespace.Name)
		}
	}
	return nil
}

func readResourceSpecs(repoFS afero.Fs) ([]*model.ResourceSpec, error) {
	resourceSpecReadWriter, err := specio.NewResourceSpecReadWriter(repoFS)
	if err != nil {
		return nil, err
	}

	return resourceSpecReadWriter.ReadAll(".")
}

func (u *uploadAllCommand) getResourceDeploymentRequest(namespaceName, storeName string,
	resources []*model.ResourceSpec,
) (*pb.DeployResourceSpecificationRequest, error) {
	resourceSpecsProto := make([]*pb.ResourceSpecification, len(resources))
	for i, resourceSpec := range resources {
		resourceSpecProto, err := resourceSpec.ToProto()
		if err != nil {
			return nil, err
		}
		resourceSpecsProto[i] = resourceSpecProto
	}

	return &pb.DeployResourceSpecificationRequest{
		Resources:     resourceSpecsProto,
		ProjectName:   u.clientConfig.Project.Name,
		DatastoreName: storeName,
		NamespaceName: namespaceName,
	}, nil
}

func (u *uploadAllCommand) getResourceStreamClient(ctx context.Context, conn *grpc.ClientConn) (pb.ResourceService_DeployResourceSpecificationClient, error) {
	client := pb.NewResourceServiceClient(conn)
	// TODO: create a new api for upload-all and remove deploy
	stream, err := client.DeployResourceSpecification(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			u.logger.Error("Deployment of resources took too long, timing out")
		}
		return nil, fmt.Errorf("deployement failed: %w", err)
	}
	return stream, nil
}

func (u *uploadAllCommand) processResourceDeploymentResponse(stream pb.ResourceService_DeployResourceSpecificationClient) error {
	u.logger.Info("> Receiving responses:")

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		if logStatus := resp.GetLogStatus(); logStatus != nil {
			if u.verbose {
				logger.PrintLogStatusVerbose(u.logger, logStatus)
			} else {
				logger.PrintLogStatus(u.logger, logStatus)
			}
			continue
		}
	}

	return nil
}
