package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/resources"
	"github.com/odpf/optimus/store/local"

	"github.com/odpf/optimus/cmd/opctl/commands"
	"github.com/odpf/optimus/core/fs"

	_ "github.com/odpf/optimus/ext/hook"
	"github.com/odpf/optimus/ext/scheduler/airflow"
	_ "github.com/odpf/optimus/ext/task"
)

var (
	// Version of the cli
	// overridden by the build system. see "Makefile"
	Version string

	//Config for optimus cli
	Config config.Opctl

	ConfigMessageErr = "unable to read optimus config file %v (%s)"
)

func main() {
	logger := log.New(os.Stderr, "", 0)
	initConfig()

	// this is just default scheduler
	// should be configurable by user if needed
	models.Scheduler = airflow.NewScheduler(resources.FileSystem, nil, nil)

	//init specs
	jobSpecRepo := local.NewJobSpecRepository(
		&fs.LocalFileSystem{BasePath: Config.Job.Path},
		local.NewAdapter(models.TaskRegistry, models.HookRegistry),
	)

	cmd := commands.New(
		logger,
		jobSpecRepo,
		Version,
		Config,
		models.Scheduler,
	)
	// error is already logged by Cobra, no need to log them again
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
		return
	}
}

func initConfig() {
	viper.SetEnvPrefix("OPTIMUS")
	viper.SetConfigName(commands.ConfigName)
	viper.SetConfigType(commands.ConfigExtension)
	if currentHomeDir, err := os.UserHomeDir(); err == nil {
		viper.AddConfigPath(filepath.Join(currentHomeDir, ".config"))
	}
	viper.AddConfigPath("/etc/")
	viper.AddConfigPath(".") // directory of binary
	viper.AddConfigPath("../")
	viper.AddConfigPath("../../") // when running in debug mode
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found
		} else {
			panic(fmt.Errorf(ConfigMessageErr, err, viper.ConfigFileUsed()))
		}
	}
	if err := viper.Unmarshal(&Config); err != nil {
		panic(fmt.Errorf(ConfigMessageErr, err, viper.ConfigFileUsed()))
	}
}
