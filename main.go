package main

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	_ "github.com/odpf/optimus/client/extension/provider"

	clientCmd "github.com/odpf/optimus/client/cmd"
	_ "github.com/odpf/optimus/ext/datastore"
	server "github.com/odpf/optimus/server/cmd"
)

var errRequestFail = errors.New("🔥 unable to complete request successfully")

//nolint:forbidigo
func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	command := clientCmd.New()

	// Add Server related commands
	command.AddCommand(
		server.NewServeCommand(),
	)

	if err := command.Execute(); err != nil {
		fmt.Println(errRequestFail)
		os.Exit(1)
	}
}
