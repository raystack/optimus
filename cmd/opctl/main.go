package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/job"

	"github.com/odpf/optimus/cmd/opctl/commands"
	"github.com/odpf/optimus/core/fs"
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
	jobSpecRepo := job.NewSpecRepository(
		&fs.LocalFileSystem{BasePath: filepath.Join(Config.Path, "jobs")},
		job.NewSpecFactory(),
	)

	dagSrv := job.NewService(
		jobSpecRepo,
	)

	cmd := commands.New(
		logger,
		dagSrv,
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
