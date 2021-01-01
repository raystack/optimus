package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/store/local"

	"github.com/odpf/optimus/cmd/opctl/commands"
	"github.com/odpf/optimus/core/fs"

	_ "github.com/odpf/optimus/ext/task"
)

var (
	// Version of the cli
	// overridden by the build system. see "Makefile"
	Version string

	//Config for optimus cli
	Config config.ConfigCLI
)

func main() {
	logger := log.New(os.Stderr, "", 0)

	//init specs
	jobSpecRepo := local.NewJobSpecRepository(
		&fs.LocalFileSystem{BasePath: filepath.Join(Config.Path, "jobs")},
	)

	cmd := commands.New(
		logger,
		jobSpecRepo,
		Version,
		Config,
	)
	// error is already logged by Cobra, no need to log them again
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
		return
	}
}
