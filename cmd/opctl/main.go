package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"os/exec"

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
	Config config.ConfigCLI
)

func main() {
	logger := log.New(os.Stderr, "", 0)
	initConfig()

	// this is just default scheduler
	// should be configurable by user if needed
	models.Scheduler = airflow.NewScheduler(resources.FileSystem, nil, nil)

	//init specs
	jobSpecRepo := local.NewJobSpecRepository(
		&fs.LocalFileSystem{BasePath: filepath.Join(Config.Path, "jobs")},
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
	viper.SetDefault("host", "https://localhost")

	viper.SetEnvPrefix("OPTIMUS")
	viper.SetConfigName("optimus")
	viper.SetConfigType("yaml")
	if currentHomeDir, err := os.UserHomeDir(); err == nil {
		viper.AddConfigPath(filepath.Join(currentHomeDir, ".config"))
	}
	viper.AddConfigPath("/etc/")
	viper.AddConfigPath(".")      // directory of binary
	viper.AddConfigPath("../../") // when running in debug mode
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found
		} else {
			panic(fmt.Errorf("unable to read optimus config file %v", err))
		}
	}
	Config.Host = viper.GetString("host")
	Config.Path = viper.GetString("path")
}

// get project name from directory using git
// remote origin url
func findSpecificationProject() (string, error) {
	absPathToSpecs, err := filepath.Abs(Config.Path)
	if err != nil {
		return "", err
	}

	gitRemoteCmdParts := strings.Split("git remote get-url origin", " ")
	gitRemoteCmd := exec.Command(gitRemoteCmdParts[0], gitRemoteCmdParts[1:]...)
	gitRemoteCmd.Dir = strings.Trim(absPathToSpecs, "\n ")
	gitRemoteURL, err := gitRemoteCmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	re := regexp.MustCompile("ocean\\/([a-zA-Z0-9-]+)(\\.git)?")
	match := re.FindStringSubmatch(string(gitRemoteURL))
	if len(match) < 2 {
		return "", errors.New("unable to find origin")
	}
	return strings.Trim(match[1], "\n "), nil
}
