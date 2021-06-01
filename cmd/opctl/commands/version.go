package commands

import (
	"context"
	"time"

	"github.com/odpf/optimus/config"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	versionTimeout = time.Second * 2
)

func versionCommand(l logger, clientVer string, conf config.Opctl) *cli.Command {
	// Version prints the current version of the tool.
	c := &cli.Command{
		Use:   "version",
		Short: "Print the client version information",
		Run: func(c *cli.Command, args []string) {
			l.Printf("client: %s", coloredNotice(clientVer))
			if conf.Host != "" {
				srvVer, err := getVersionRequest(clientVer, conf.Host)
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						return
					}
					panic(err)
				}
				l.Printf("server: %s", coloredNotice(srvVer))
			}
		},
	}
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
