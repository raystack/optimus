package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/odpf/optimus/models"

	"github.com/odpf/optimus/config"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	versionTimeout = time.Second * 2
)

func versionCommand(l logger, host string, pluginRepo models.PluginRepository) *cli.Command {
	var serverVersion bool
	c := &cli.Command{
		Use:   "version",
		Short: "Print the client version information",
		RunE: func(c *cli.Command, args []string) error {
			l.Printf(fmt.Sprintf("client: %s-%s", coloredNotice(config.Version), config.BuildCommit))

			if host != "" && serverVersion {
				srvVer, err := getVersionRequest(config.Version, host)
				if err != nil {
					return err
				}
				l.Printf("server: %s", coloredNotice(srvVer))
			}

			l.Println("\nDiscovered plugins:")
			for taskIdx, tasks := range pluginRepo.GetAll() {
				schema := tasks.Info()
				l.Printf("%d. %s\n", taskIdx+1, schema.Name)
				l.Printf("Description: %s\n", schema.Description)
				l.Printf("Image: %s\n", schema.Image)
				l.Printf("Type: %s\n", schema.PluginType)
				l.Printf("Plugin version: %s\n", schema.PluginVersion)
				l.Printf("Plugin mods: %v\n", schema.PluginMods)
				if schema.HookType != "" {
					l.Printf("Hook type: %s\n", schema.HookType)
				}
				if len(schema.DependsOn) != 0 {
					l.Printf("Depends on: %v\n", schema.DependsOn)
				}
				l.Println("")
			}
			return nil
		},
	}
	c.Flags().BoolVar(&serverVersion, "with-server", false, "check for server version")
	return c
}

// getVersionRequest send a job request to service
func getVersionRequest(clientVer string, host string) (ver string, err error) {
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

	versionResponse, err := runtime.Version(ctx, &pb.VersionRequest{
		Client: clientVer,
	})
	if err != nil {
		return "", errors.Wrapf(err, "request failed for version")
	}
	return versionResponse.Server, nil
}
