package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/hashicorp/go-hclog"

	"github.com/odpf/optimus/plugin"

	hPlugin "github.com/hashicorp/go-plugin"

	"github.com/spf13/viper"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/resources"
	"github.com/odpf/optimus/store/local"

	"github.com/odpf/optimus/cmd/opctl/commands"
	"github.com/odpf/optimus/core/fs"

	lg "github.com/odpf/optimus/core/logger"
	_ "github.com/odpf/optimus/ext/datastore"
	"github.com/odpf/optimus/ext/scheduler/airflow"
	_ "github.com/odpf/optimus/plugin"
)

var (
	// Version of the cli
	// overridden by the build system. see "Makefile"
	Version string

	//Config for optimus cli
	Config config.Opctl

	ConfigMessageErr = "unable to read optimus config file %v (%s)"
	EnvLogLevel      = "LOG_LEVEL"
)

func main() {
	initConfig()
	pluginLogLevel := hclog.Info
	if level := viper.GetString(EnvLogLevel); level != "" {
		lg.Init(level)
		if level == "DEBUG" {
			pluginLogLevel = hclog.Debug
		}
	} else {
		lg.Init(lg.INFO)
	}

	// this is just default scheduler
	// should be configurable by user if needed
	models.Scheduler = airflow.NewScheduler(resources.FileSystem, nil, nil)

	//init specs
	jobSpecRepo := local.NewJobSpecRepository(
		&fs.LocalFileSystem{BasePath: Config.Job.Path},
		local.NewJobSpecAdapter(models.TaskRegistry, models.HookRegistry),
	)
	datastoreSpecsFs := map[string]fs.FileSystem{}
	for _, dsConfig := range Config.Datastore {
		datastoreSpecsFs[dsConfig.Type] = &fs.LocalFileSystem{
			BasePath: dsConfig.Path,
		}
	}

	// Create an hclog.Logger
	pluginLogger := hclog.New(&hclog.LoggerOptions{
		Name:   "optimus",
		Output: os.Stdout,
		Level:  pluginLogLevel,
	})
	plugin.Initialize(pluginLogger)
	// Make sure we clean up any managed plugins at the end of this
	defer hPlugin.CleanupClients()

	cmd := commands.New(
		log.New(os.Stderr, "", 0),
		jobSpecRepo,
		Version,
		Config,
		models.Scheduler,
		datastoreSpecsFs,
		models.TaskRegistry,
		models.HookRegistry,
		models.DatastoreRegistry,
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
