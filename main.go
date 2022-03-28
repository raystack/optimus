package main

import (
	"errors"
	"fmt"
	"math/rand"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/odpf/optimus/cmd"
	_ "github.com/odpf/optimus/ext/datastore"
	"github.com/odpf/optimus/models"
)

var errRequestFail = errors.New("🔥 unable to complete request successfully")

//nolint:forbidigo
func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	command := cmd.New(
		models.PluginRegistry,
		models.DatastoreRegistry,
	)

	if err := command.Execute(); err != nil {
		fmt.Println(errRequestFail)
		os.Exit(1)
	}
}
