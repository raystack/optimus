package main

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/odpf/optimus/cmd"
	_ "github.com/odpf/optimus/ext/datastore"
	_ "github.com/odpf/optimus/ext/resourcemgr"
	_ "github.com/odpf/optimus/extension/provider"
)

var errRequestFail = errors.New("🔥 unable to complete request successfully")

//nolint:forbidigo
func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	command := cmd.New()

	if err := command.Execute(); err != nil {
		fmt.Println(errRequestFail)
		os.Exit(1)
	}
}
