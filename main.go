package main

import (
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/odpf/optimus/cmd"
	_ "github.com/odpf/optimus/ext/datastore"
	"github.com/odpf/optimus/models"
)

//nolint:forbidigo
func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	command := cmd.New(
		models.PluginRegistry,
		models.DatastoreRegistry,
	)

	if err := command.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
