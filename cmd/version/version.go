package version

import (
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"github.com/odpf/salt/version"
	"github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/cmd/internal"
	"github.com/odpf/optimus/cmd/internal/connectivity"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/plugin"
	"github.com/odpf/optimus/cmd/progressbar"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

const versionTimeout = time.Second * 2

type versionCommand struct {
	logger         log.Logger
	configFilePath string

	isWithServer bool
	host         string

	pluginCleanFn func()
}

// NewVersionCommand initializes command to get version
func NewVersionCommand() *cobra.Command {
	v := &versionCommand{}

	cmd := &cobra.Command{
		Use:      "version",
		Short:    "Print the client version information",
		Example:  "optimus version [--with-server]",
		RunE:     v.RunE,
		PreRunE:  v.PreRunE,
		PostRunE: v.PostRunE,
	}

	v.injectFlags(cmd)

	return cmd
}

func (v *versionCommand) injectFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&v.isWithServer, "with-server", v.isWithServer, "Check for server version")

	// Config filepath flag
	cmd.Flags().StringVarP(&v.configFilePath, "config", "c", config.EmptyPath, "File path for client configuration")

	// Mandatory flags if with-server is set but config is not set
	cmd.Flags().StringVar(&v.host, "host", "", "Optimus service endpoint url")
}

func (v *versionCommand) PreRunE(cmd *cobra.Command, _ []string) error {
	v.logger = logger.NewDefaultLogger()

	if v.isWithServer {
		conf, err := internal.LoadOptionalConfig(v.configFilePath)
		if err != nil {
			return err
		}

		if conf == nil {
			cmd.MarkFlagRequired("host")
		} else {
			v.logger = logger.NewClientLogger(conf.Log)
			if v.host == "" {
				v.host = conf.Host
			}
		}
	}

	var err error
	v.pluginCleanFn, err = plugin.TriggerClientPluginsInit(config.LogLevel(v.logger.Level()))
	return err
}

func (v *versionCommand) RunE(_ *cobra.Command, _ []string) error {
	// Print client version
	v.logger.Info(fmt.Sprintf("Client: %s-%s", logger.ColoredNotice(config.BuildVersion), logger.ColoredNotice(config.BuildCommit)))

	// Print server version
	if v.isWithServer {
		srvVer, err := v.getVersionRequest(config.BuildVersion, v.host)
		if err != nil {
			return err
		}
		v.logger.Info(fmt.Sprintf("Server: %s", logger.ColoredNotice(srvVer)))
	}

	// Print version update if new version is exist
	githubRepo := "odpf/optimus"
	if updateNotice := version.UpdateNotice(config.BuildVersion, githubRepo); updateNotice != "" {
		v.logger.Info(updateNotice)
	}
	v.printAllPluginInfos()
	return nil
}

func (v *versionCommand) PostRunE(_ *cobra.Command, _ []string) error {
	v.pluginCleanFn()
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
func (*versionCommand) getVersionRequest(clientVer, host string) (ver string, err error) {
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
