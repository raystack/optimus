package commands

import (
	"context"

	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
	pb "github.com/odpf/optimus/api/proto/v1"
)

func versionCommand(l logger, clientVer string) *cli.Command {
	// Version prints the current version of the tool.
	c := &cli.Command{
		Use:   "version",
		Short: "Print the client version information",
		Run: func(c *cli.Command, args []string) {
			l.Printf("client: %s", clientVer)
			if optimusHost != "" {
				srvVer, err := getVersionRequest(l, clientVer)
				if err != nil {
					panic(err)
				}
				l.Printf("server: %s", srvVer)
			}
		},
	}
	c.Flags().StringVar(&optimusHost, "host", "", "deployment service endpoint url")
	return c
}

// getVersionRequest send a job request to service
func getVersionRequest(l logger, clientVer string) (ver string, err error) {
	var conn *grpc.ClientConn
	if conn, err = createConnection(optimusHost); err != nil {
		return "", err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
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
