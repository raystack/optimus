package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/odpf/salt/log"
	"github.com/odpf/salt/version"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
)

const (
	versionTimeout = time.Second * 2
	githubRepo     = "odpf/optimus"
)

func versionCommand(l log.Logger, host string, pluginRepo models.PluginRepository) *cli.Command {
	var serverVersion bool
	c := &cli.Command{
		Use:     "version",
		Short:   "Print the client version information",
		Example: "optimus version [--with-server]",
		RunE: func(c *cli.Command, args []string) error {
			l.Info(fmt.Sprintf("Client: %s-%s", coloredNotice(config.Version),
				coloredNotice(config.BuildCommit)))
			if host != "" && serverVersion {
				srvVer, err := getVersionRequest(config.Version, host)
				if err != nil {
					return err
				}
				l.Info(fmt.Sprintf("Server: %s", coloredNotice(srvVer)))
			}
			if updateNotice := version.UpdateNotice(config.Version, githubRepo); updateNotice != "" {
				l.Info(updateNotice)
			}

			plugins := pluginRepo.GetAll()
			l.Info(fmt.Sprintf("\nDiscovered plugins: %d", len(plugins)))
			for taskIdx, tasks := range plugins {
				schema := tasks.Info()
				l.Info(fmt.Sprintf("\n%d. %s", taskIdx+1, schema.Name))
				l.Info(fmt.Sprintf("Description: %s", schema.Description))
				l.Info(fmt.Sprintf("Image: %s", schema.Image))
				l.Info(fmt.Sprintf("Type: %s", schema.PluginType))
				l.Info(fmt.Sprintf("Plugin version: %s", schema.PluginVersion))
				l.Info(fmt.Sprintf("Plugin mods: %v", schema.PluginMods))
				if schema.HookType != "" {
					l.Info(fmt.Sprintf("Hook type: %s", schema.HookType))
				}
				if len(schema.DependsOn) != 0 {
					l.Info(fmt.Sprintf("Depends on: %v", schema.DependsOn))
				}
			}
			return nil
		},
	}
	c.Flags().BoolVar(&serverVersion, "with-server", false, "Check for server version")
	return c
}

// getVersionRequest send a version request to service
func getVersionRequest(clientVer, host string) (ver string, err error) {
	dialTimeoutCtx, dialCancel := context.WithTimeout(context.Background(), OptimusDialTimeout)
	defer dialCancel()

	var conn *grpc.ClientConn
	if conn, err = createConnection(dialTimeoutCtx, host); err != nil {
		return "", err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), versionTimeout)
	defer cancel()

	runtime := pb.NewRuntimeServiceClient(conn)
	spinner := NewProgressBar()
	spinner.Start("please wait...")
	versionResponse, err := runtime.Version(ctx, &pb.VersionRequest{
		Client: clientVer,
	})
	if err != nil {
		return "", fmt.Errorf("request failed for version: %w", err)
	}
	time.Sleep(versionTimeout)
	spinner.Stop()
	return versionResponse.Server, nil
}
