package plugin

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/spf13/cobra"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/plugin"
)

// NewInstallCommand initializes plugin install command
func NewSyncCommand() *cobra.Command {
	sync := &syncCommand{
		clientConfig: &config.ClientConfig{},
	}
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
}

func (s *syncCommand) PreRunE(_ *cobra.Command, _ []string) error {
	c, err := config.LoadClientConfig(s.configFilePath)
	if err != nil {
		return err
	}
	*s.clientConfig = *c
	return nil
}

func downloadArchiveFromServer(conf config.ClientConfig) error {
	optimusServerURL := fmt.Sprintf("http://%s/%s", conf.Host, "plugins") // will url work ?

	req, err := http.NewRequestWithContext(context.Background(), "GET", optimusServerURL, nil)
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
	err := downloadArchiveFromServer(*s.clientConfig)
	if err != nil {
		return err
	}
	return plugin.NewPluginManager().UnArchive(
		plugin.PluginsArchiveName,
		plugin.PluginsDir,
	)
}
