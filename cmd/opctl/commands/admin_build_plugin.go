package commands

import (
	"io/ioutil"

	"github.com/odpf/optimus/instance"

	"github.com/odpf/optimus/plugin"

	cli "github.com/spf13/cobra"
)

var (
	templateEngine = instance.NewGoEngine()
)

func adminBuildPluginCommand(l logger) *cli.Command {
	var (
		configPath       string
		opctlDownloadUrl string
		binaryBuildPath  string

		skipDockerBuild bool
		skipBinaryBuild bool
	)
	cmd := &cli.Command{
		Use:     "plugin",
		Short:   "Build docker wrappers for plugins",
		Example: "opctl admin build plugin --config ./config.yaml --output . --opctl-url https://example.io",
	}

	cmd.Flags().StringVar(&configPath, "config", "", "config file path")
	cmd.MarkFlagRequired("config")
	cmd.Flags().StringVar(&opctlDownloadUrl, "opctl-url", "", "opctl download url")
	cmd.MarkFlagRequired("opctl-url")
	cmd.Flags().StringVar(&binaryBuildPath, "output", "./dist", "binary build directory")
	cmd.Flags().BoolVar(&skipDockerBuild, "skip-docker-build", false, "skip building docker images")
	cmd.Flags().BoolVar(&skipBinaryBuild, "skip-binary-build", false, "skip building binary")

	cmd.Run = func(c *cli.Command, args []string) {
		l.Printf("generating docker files for plugins, using config %s\nplease wait...\n", configPath)

		if err := getPluginBuildRequest(configPath, binaryBuildPath, opctlDownloadUrl, skipDockerBuild, skipBinaryBuild); err != nil {
			errExit(l, err)
		}
	}
	return cmd
}

func getPluginBuildRequest(configPath, binaryBuildPath, opctlDownloadUrl string, skipDockerBuild, skipBinaryBuild bool) error {
	configBytes, err := ioutil.ReadFile(configPath)
	if err != nil {
		return err
	}
	return plugin.BuildHelper(templateEngine, configBytes, binaryBuildPath, opctlDownloadUrl, skipDockerBuild, skipBinaryBuild)
}
