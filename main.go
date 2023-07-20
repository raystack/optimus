package main

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"time"

	_ "go.uber.org/automaxprocs"

	clientCmd "github.com/raystack/optimus/client/cmd"
	_ "github.com/raystack/optimus/client/extension/provider"
	server "github.com/raystack/optimus/server/cmd"
	"github.com/raystack/optimus/server/cmd/migration"
)

var errRequestFail = errors.New("🔥 unable to complete request successfully")

//nolint:forbidigo
func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	command := clientCmd.New()

	// Add Server related commands
	command.AddCommand(
		server.NewServeCommand(),
		migration.NewMigrationCommand(),
	)

	if err := command.Execute(); err != nil {
		fmt.Println(errRequestFail)
		os.Exit(1)
	}
}
