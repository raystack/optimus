package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/odpf/optimus/plugin"

	hPlugin "github.com/hashicorp/go-plugin"

	"github.com/odpf/optimus/cmd"
	"github.com/odpf/optimus/config"
	lg "github.com/odpf/optimus/core/logger"
	_ "github.com/odpf/optimus/ext/datastore"
	"github.com/odpf/optimus/models"
	_ "github.com/odpf/optimus/plugin"
)

var (
	errRequestFail = errors.New("🔥 unable to complete request successfully")
)

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	configuration, err := config.InitOptimus()
	if err != nil {
		fmt.Printf("ERROR: %s\n", err.Error())
		os.Exit(1)
	}

	pluginLogLevel := hclog.Info
	if configuration.GetLog().Level != "" {
		lg.Init(configuration.GetLog().Level)
		if strings.ToLower(configuration.GetLog().Level) == "debug" {
			pluginLogLevel = hclog.Debug
		}
	} else {
		lg.Init(lg.INFO)
	}

	// discover and load plugins
	if err := plugin.Initialize(hclog.New(&hclog.LoggerOptions{
		Name:   "optimus",
		Output: os.Stdout,
		Level:  pluginLogLevel,
	})); err != nil {
		hPlugin.CleanupClients()
		fmt.Printf("ERROR: %s\n", err.Error())
		os.Exit(1)
	}
	// Make sure we clean up any managed plugins at the end of this
	defer hPlugin.CleanupClients()

	command := cmd.New(
		log.New(os.Stderr, "", 0),
		configuration,
		models.TaskRegistry,
		models.HookRegistry,
		models.DatastoreRegistry,
	)
	if err := command.Execute(); err != nil {
		hPlugin.CleanupClients()
		fmt.Println(errRequestFail)
		os.Exit(1)
	}
}
