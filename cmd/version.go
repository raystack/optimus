package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/hashicorp/go-version"
	pb "github.com/odpf/optimus/api/proto/odpf/optimus"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const (
	versionTimeout   = time.Second * 2
	clientVersion    = time.Second * 1
	githubReleaseURL = "https://api.github.com/repos/odpf/optimus/releases/latest"
)

func versionCommand(l log.Logger, host string, pluginRepo models.PluginRepository) *cli.Command {
	var serverVersion bool
	c := &cli.Command{
		Use:   "version",
		Short: "Print the client version information",
		RunE: func(c *cli.Command, args []string) error {
			l.Info(fmt.Sprintf(coloredShow("client: %s-%s"), coloredNotice(config.Version), config.BuildCommit))
			if host != "" && serverVersion {
				srvVer, err := getVersionRequest(config.Version, host)
				if err != nil {
					return err
				}
				l.Info(fmt.Sprintf("server: %s", coloredNotice(srvVer)))
			}
			checkLatestVersion(l)

			plugins := pluginRepo.GetAll()
			l.Info(fmt.Sprintf(coloredShow("\nDiscovered plugins: %d"), len(plugins)))
			for taskIdx, tasks := range plugins {
				schema := tasks.Info()
				l.Info(fmt.Sprintf(coloredPrint("\n%d. %s"), taskIdx+1, schema.Name))
				l.Info(fmt.Sprintf(coloredShow("Description: %s"), schema.Description))
				l.Info(fmt.Sprintf(coloredShow("Image: %s"), schema.Image))
				l.Info(fmt.Sprintf(coloredShow("Type: %s"), schema.PluginType))
				l.Info(fmt.Sprintf(coloredShow("Plugin version: %s"), schema.PluginVersion))
				l.Info(fmt.Sprintf(coloredShow("Plugin mods: %v"), schema.PluginMods))
				if schema.HookType != "" {
					l.Info(fmt.Sprintf(coloredShow("Hook type: %s"), schema.HookType))
				}
				if len(schema.DependsOn) != 0 {
					l.Info(fmt.Sprintf(coloredShow("Depends on: %v"), schema.DependsOn))
				}
			}
			return nil
		},
	}
	c.Flags().BoolVar(&serverVersion, "with-server", false, "check for server version")
	return c
}

func checkLatestVersion(l log.Logger) {
	gitClient := http.Client{
		Timeout: clientVersion,
	}

	req, err := http.NewRequest(http.MethodGet, githubReleaseURL, nil)
	if err != nil {
		l.Info("failed to create request for latest version")
		return
	}
	req.Header.Set("User-Agent", "optimus")
	res, err := gitClient.Do(req)
	if err != nil {
		l.Info("failed to get latest version from github")
		return
	}
	if res.StatusCode != http.StatusOK {
		l.Info("failed to get latest version from github")
		return
	}
	if res.Body != nil {
		defer res.Body.Close()
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		l.Info("failed to read response body")
		return
	}

	authorType := struct {
		TagName string `json:"tag_name"`
	}{}
	if err = json.Unmarshal(body, &authorType); err != nil {
		l.Info(fmt.Sprintf("failed to parse: %s", string(body)))
		return
	}

	currentV, err := version.NewVersion(config.Version)
	if err != nil {
		l.Info(fmt.Sprintf("failed to parse current version %s", err))
		return
	}
	latestV, err := version.NewVersion(authorType.TagName)
	if err != nil {
		l.Info(fmt.Sprintf("failed to parse latest version %s", err))
		return
	}

	if currentV.LessThan(latestV) {
		l.Info(fmt.Sprintf("new version is available: %s, consider updating the client", coloredNotice(latestV)))
	}
}

// getVersionRequest send a version request to service
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
