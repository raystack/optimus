package plugin

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/odpf/salt/log"
	"github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/internal/logger"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/plugin"
)

func NewSyncCommand() *cobra.Command {
	sync := &syncCommand{}
	cmd := &cobra.Command{
		Use:     "sync",
		Short:   "sync plugins from server",
		Example: "optimus plugin sync -c optimus.yaml",
		PreRunE: sync.PreRunE,
		RunE:    sync.RunE,
	}
	cmd.PersistentFlags().StringVarP(&sync.configFilePath, "config", "c", sync.configFilePath, "File path for optimus configuration")
	return cmd
}

type syncCommand struct {
	configFilePath string
	clientConfig   *config.ClientConfig
	logger         log.Logger
}

func (s *syncCommand) PreRunE(_ *cobra.Command, _ []string) error {
	c, err := config.LoadClientConfig(s.configFilePath)
	if err != nil {
		return err
	}
	s.clientConfig = c
	s.logger = logger.NewClientLogger(c.Log)
	return nil
}

func getPluginDownloadURL(host string) (*url.URL, error) {
	var downloadURL *url.URL
	var err error
	pluginPath := "plugins"
	if strings.HasPrefix(host, "http://") || strings.HasPrefix(host, "https://") {
		downloadURL, err = url.Parse(host)
		if err != nil {
			return nil, err
		}
		downloadURL.Path = pluginPath
	} else {
		downloadURL = &url.URL{
			Scheme: "http",
			Host:   host,
			Path:   pluginPath,
		}
	}
	return downloadURL, nil
}

func (s *syncCommand) downloadArchiveFromServer() error {
	downloadURL, err := getPluginDownloadURL(s.clientConfig.Host)
	s.logger.Info(fmt.Sprintf("download URL : %s", downloadURL.String()))
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(context.Background(), "GET", downloadURL.String(), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	out, err := os.Create(plugin.PluginsArchiveName)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, resp.Body)
	return err
}

func (s *syncCommand) RunE(_ *cobra.Command, _ []string) error {
	err := s.downloadArchiveFromServer()
	if err != nil {
		return err
	}
	return plugin.NewPluginManager().UnArchive(
		plugin.PluginsArchiveName,
		plugin.PluginsDir,
	)
}
