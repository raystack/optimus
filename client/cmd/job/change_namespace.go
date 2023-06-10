package job

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/goto/salt/log"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/cmd/internal"
	"github.com/goto/optimus/client/cmd/internal/connectivity"
	"github.com/goto/optimus/client/cmd/internal/logger"
	"github.com/goto/optimus/client/local/specio"
	"github.com/goto/optimus/config"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

const (
	changeNamespaceTimeout = time.Minute * 1
)

type changeNamespaceCommand struct {
	logger         log.Logger
	configFilePath string
	clientConfig   *config.ClientConfig

	project          string
	oldNamespaceName string
	newNamespaceName string
	host             string
}

// NewChangeNemespaceCommand initializes job namespace change command
func NewChangeNemespaceCommand() *cobra.Command {
	l := logger.NewClientLogger()
	changeNamespace := &changeNamespaceCommand{
		logger: l,
	}
	cmd := &cobra.Command{
		Use:      "change-namespace",
		Short:    "Change namespace of a Job",
		Example:  "optimus job change-namespace <job-name> --old-namespace <old-namespace> --new-namespace <new-namespace>",
		Args:     cobra.MinimumNArgs(1),
		PreRunE:  changeNamespace.PreRunE,
		RunE:     changeNamespace.RunE,
		PostRunE: changeNamespace.PostRunE,
	}
	// Config filepath flag
	cmd.Flags().StringVarP(&changeNamespace.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")
	internal.MarkFlagsRequired(cmd, []string{"old-namespace", "new-namespace"})
	changeNamespace.injectFlags(cmd)

	return cmd
}

func (c *changeNamespaceCommand) injectFlags(cmd *cobra.Command) {
	// Mandatory flags
	cmd.Flags().StringVarP(&c.oldNamespaceName, "old-namespace", "o", "", "current namespace of the job")
	cmd.Flags().StringVarP(&c.newNamespaceName, "new-namespace", "n", "", "namespace to which the job needs to be moved to")

	// Mandatory flags if config is not set
	cmd.Flags().StringVarP(&c.project, "project-name", "p", "", "Name of the optimus project")
	cmd.Flags().StringVar(&c.host, "host", "", "Optimus service endpoint url")
}

func (c *changeNamespaceCommand) PreRunE(_ *cobra.Command, _ []string) error {
	// Load mandatory config
	conf, err := config.LoadClientConfig(c.configFilePath)
	if err != nil {
		return err
	}

	c.clientConfig = conf
	return err
}

func (c *changeNamespaceCommand) RunE(_ *cobra.Command, args []string) error {
	jobName := args[0]
	err := c.sendChangeNamespaceRequest(jobName)
	if err != nil {
		return fmt.Errorf("namespace change request failed for job %s: %w", jobName, err)
	}
	c.logger.Info("[OK] Successfully changed namespace and deployed new DAG on Scheduler")
	return nil
}

func (c *changeNamespaceCommand) sendChangeNamespaceRequest(jobName string) error {
	conn, err := connectivity.NewConnectivity(c.host, changeNamespaceTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	// fetch Instance by calling the optimus API
	jobRunServiceClient := pb.NewJobSpecificationServiceClient(conn.GetConnection())
	request := &pb.ChangeJobNamespaceRequest{
		ProjectName:      c.project,
		NamespaceName:    c.oldNamespaceName,
		NewNamespaceName: c.newNamespaceName,
		JobName:          jobName,
	}

	_, err = jobRunServiceClient.ChangeJobNamespace(conn.GetContext(), request)
	return err
}

func (c *changeNamespaceCommand) PostRunE(_ *cobra.Command, args []string) error {
	c.logger.Info("\n[info] Moving job in filesystem")
	jobName := args[0]
	jobSpecReadWriter, err := specio.NewJobSpecReadWriter(afero.NewOsFs())
	if err != nil {
		return err
	}

	oldNamespaceConfig, err := c.getNamespaceConfig(c.oldNamespaceName)
	if err != nil {
		return errors.Wrap(tenant.EntityNamespace, "unregistered old namespace", err)
	}

	jobSpec, err := jobSpecReadWriter.ReadByName(oldNamespaceConfig.Job.Path, jobName)
	if err != nil {
		return errors.Wrap(tenant.EntityNamespace, "unable to find job in old namespace", err)
	}

	fs := afero.NewOsFs()
	newNamespaceConfig, err := c.getNamespaceConfig(c.newNamespaceName)
	if err != nil || newNamespaceConfig.Job.Path == "" {
		c.logger.Warn("[warn] new namespace not recognised for jobs")
		c.logger.Warn("[info] removing job from old namespace")
		err = fs.RemoveAll(jobSpec.Path)
		if err != nil {
			c.logger.Warn("unable to remove job from old namespace")
			return errors.NewError(errors.ErrInternalError, "change-namespace", "unable to remove job from old namespace")
		}
		c.logger.Warn("[info] removed job spec from current namespace directory")
		c.logger.Warn("[info] run `optimus job export` on the new namespace repo, to fetch the newly moved job.")
		c.logger.Info("[OK] Job moved successfully")
		return nil
	}

	var relativeJobPath string
	splitComp := strings.Split(jobSpec.Path, oldNamespaceConfig.Job.Path)
	if !(len(splitComp) > 1) {
		return errors.NewError(errors.ErrInternalError, "change-namespace", "unable to parse job spec path")
	}
	relativeJobPath = splitComp[1]

	c.logger.Info(fmt.Sprintf("\t* Old Path : '%s' \n\t* New Path : '%s' \n", jobSpec.Path, newNamespaceConfig.Job.Path+relativeJobPath))

	c.logger.Info(fmt.Sprintf("[info] creating job directry: %s", newNamespaceConfig.Job.Path+relativeJobPath))

	err = fs.MkdirAll(filepath.Dir(newNamespaceConfig.Job.Path+relativeJobPath), os.ModePerm)
	if err != nil {
		return err
	}

	err = fs.Rename(jobSpec.Path, newNamespaceConfig.Job.Path+relativeJobPath)
	if err != nil {
		return err
	}
	c.logger.Info("[OK] Job moved successfully")
	return nil
}

func (c *changeNamespaceCommand) getNamespaceConfig(namespaceName string) (*config.Namespace, error) {
	for _, namespace := range c.clientConfig.Namespaces {
		if namespace.Name == namespaceName {
			return namespace, nil
		}
	}
	return nil, errors.NotFound(tenant.EntityNamespace, "not recognised in config")
}
