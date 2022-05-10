package version

import (
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"github.com/odpf/salt/version"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/plugin"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

const versionTimeout = time.Second * 2

type versionCommand struct {
	logger log.Logger

	isWithServer   bool
	configFilePath string

	pluginCleanFn func()
}

// NewVersionCommand initializes command to get version
func NewVersionCommand() *cobra.Command {
	version := &versionCommand{
		logger: logger.NewDefaultLogger(),
	}

	cmd := &cobra.Command{
		Use:      "version",
		Short:    "Print the client version information",
		Example:  "optimus version [--with-server]",
		RunE:     version.RunE,
		PreRunE:  version.PreRunE,
		PostRunE: version.PostRunE,
	}

	cmd.Flags().BoolVar(&version.isWithServer, "with-server", version.isWithServer, "Check for server version")
	cmd.Flags().StringVarP(&version.configFilePath, "config", "c", version.configFilePath, "File path for client configuration")
	return cmd
}

func (v *versionCommand) PreRunE(cmd *cobra.Command, args []string) error {
	var err error
	v.pluginCleanFn, err = plugin.TriggerClientPluginsInit(config.LogLevelInfo)
	return err
}

func (v *versionCommand) PostRunE(cmd *cobra.Command, args []string) error {
	v.pluginCleanFn()
	return nil
}

func (v *versionCommand) RunE(cmd *cobra.Command, args []string) error {
	// Print client version
	v.logger.Info(fmt.Sprintf("Client: %s-%s", config.BuildVersion, config.BuildCommit))

	// Print server version
	if v.isWithServer {
		// TODO: find a way to load the config in one place
		clientConfig, err := config.LoadClientConfig(v.configFilePath, cmd.Flags())
		if err != nil {
			return err
		}
		if err := config.ValidateClientConfig(clientConfig); err != nil { // experiment for client validation
			return err
		}

		v.logger = logger.NewClientLogger(clientConfig.Log)
		srvVer, err := v.getVersionRequest(config.BuildVersion, clientConfig.Host)
		if err != nil {
			return err
		}
		v.logger.Info(fmt.Sprintf("Server: %s", srvVer))
	}

	// Print version update if new version is exist
	githubRepo := "odpf/optimus"
	if updateNotice := version.UpdateNotice(config.BuildVersion, githubRepo); updateNotice != "" {
		v.logger.Info(updateNotice)
	}
	v.printAllPluginInfos()
	return nil
}

func (v *versionCommand) printAllPluginInfos() {
	pluginRepo := models.PluginRegistry
	plugins := pluginRepo.GetAll()
	v.logger.Info(fmt.Sprintf("\nDiscovered plugins: %d", len(plugins)))
	for taskIdx, tasks := range plugins {
		schema := tasks.Info()
		v.logger.Info(fmt.Sprintf("\n%d. %s", taskIdx+1, schema.Name))
		v.logger.Info(fmt.Sprintf("Description: %s", schema.Description))
		v.logger.Info(fmt.Sprintf("Image: %s", schema.Image))
		v.logger.Info(fmt.Sprintf("Type: %s", schema.PluginType))
		v.logger.Info(fmt.Sprintf("Plugin version: %s", schema.PluginVersion))
		v.logger.Info(fmt.Sprintf("Plugin mods: %v", schema.PluginMods))
		if schema.HookType != "" {
			v.logger.Info(fmt.Sprintf("Hook type: %s", schema.HookType))
		}
		if len(schema.DependsOn) != 0 {
			v.logger.Info(fmt.Sprintf("Depends on: %v", schema.DependsOn))
		}
	}
}

// getVersionRequest send a version request to service
func (v *versionCommand) getVersionRequest(clientVer, host string) (ver string, err error) {
	conn, err := connectivity.NewConnectivity(host, versionTimeout)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	runtime := pb.NewRuntimeServiceClient(conn.GetConnection())
	spinner := progressbar.NewProgressBar()
	spinner.Start("please wait...")
	versionResponse, err := runtime.Version(conn.GetContext(), &pb.VersionRequest{
		Client: clientVer,
	})
	if err != nil {
		return "", fmt.Errorf("request failed for version: %w", err)
	}
	time.Sleep(versionTimeout)
	spinner.Stop()
	return versionResponse.Server, nil
}
