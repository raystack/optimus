package resource

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/salt/log"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/cmd/internal/progressbar"
	"github.com/goto/optimus/client/cmd/internal/survey"
	"github.com/goto/optimus/config"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	applyTimeout  = time.Minute * 5
	successStatus = "success"
)

type applyCommand struct {
	logger     log.Logger
	connection connection.Connection

	configFilePath string
	clientConfig   *config.ClientConfig

	namespaceSurvey *survey.NamespaceSurvey
	namespaceName   string
	projectName     string
	storeName       string

	verbose       bool
	resourceNames []string
}

// NewApplyCommand initializes command for applying resources from optimus to datastore
func NewApplyCommand() *cobra.Command {
	l := logger.NewClientLogger()
	apply := &applyCommand{
		logger:          l,
		namespaceSurvey: survey.NewNamespaceSurvey(l),
	}

	cmd := &cobra.Command{
		Use:     "apply",
		Short:   "Apply resources from optimus to datastore",
		Long:    heredoc.Doc(`Apply changes to destination datastore`),
		Example: "optimus resource apply <resource-name1,resource-name2>",
		Annotations: map[string]string{
			"group:core": "true",
		},
		RunE:    apply.RunE,
		PreRunE: apply.PreRunE,
	}
	cmd.Flags().StringVarP(&apply.configFilePath, "config", "c", apply.configFilePath, "File path for client configuration")
	cmd.Flags().StringSliceVarP(&apply.resourceNames, "resource-names", "R", nil, "Selected resources of optimus project")
	cmd.Flags().BoolVarP(&apply.verbose, "verbose", "v", false, "Print details related to upload-all stages")
	cmd.Flags().StringVarP(&apply.namespaceName, "namespace", "n", "", "Namespace name within project")
	cmd.Flags().StringVarP(&apply.storeName, "datastore", "s", "bigquery", "Datastore type where the resource belongs")
	return cmd
}

func (a *applyCommand) PreRunE(_ *cobra.Command, _ []string) error {
	var err error
	a.clientConfig, err = config.LoadClientConfig(a.configFilePath)
	if err != nil {
		return err
	}

	a.connection = connection.New(a.logger, a.clientConfig)

	return nil
}

func (a *applyCommand) RunE(_ *cobra.Command, _ []string) error {
	a.logger.Info("> Validating resource names")
	if len(a.resourceNames) == 0 {
		return errors.New("empty resource names")
	}

	if a.projectName == "" {
		a.projectName = a.clientConfig.Project.Name
	}

	var namespace *config.Namespace
	// use flag or ask namespace name
	if a.namespaceName == "" {
		var err error
		namespace, err = a.namespaceSurvey.AskToSelectNamespace(a.clientConfig)
		if err != nil {
			return err
		}
		a.namespaceName = namespace.Name
	}

	return a.apply()
}

func (a *applyCommand) apply() error {
	conn, err := a.connection.Create(a.clientConfig.Host)
	if err != nil {
		return err
	}
	defer conn.Close()

	apply := pb.NewResourceServiceClient(conn)

	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")

	applyRequest := pb.ApplyResourcesRequest{
		ProjectName:   a.projectName,
		NamespaceName: a.namespaceName,
		DatastoreName: a.storeName,
		ResourceNames: a.resourceNames,
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), applyTimeout)
	defer cancelFunc()

	responses, err := apply.ApplyResources(ctx, &applyRequest)
	spinner.Stop()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			a.logger.Error("Apply took too long, timing out")
		}
		return fmt.Errorf("failed to apply resourcse: %w", err)
	}

	a.printApplyStatus(responses)
	return nil
}

func (a *applyCommand) printApplyStatus(responses *pb.ApplyResourcesResponse) {
	a.logger.Info("Apply finished")
	var successResources []string
	for _, status := range responses.Statuses {
		if status.Status == successStatus {
			successResources = append(successResources, status.ResourceName)
		}
	}
	if len(successResources) > 0 {
		a.logger.Info("Resources with success")
		for i, name := range successResources {
			a.logger.Info("%d. %s", i+1, name)
		}
	}

	for _, resourceStatus := range responses.Statuses {
		if resourceStatus.Status != successStatus {
			a.logger.Error("Resource [%s] failed with: %s", resourceStatus.ResourceName, resourceStatus.Reason)
		}
	}
}
