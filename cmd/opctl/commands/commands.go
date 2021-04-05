package commands

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/fatih/color"
	cli "github.com/spf13/cobra"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

var prologueContents = `opctl %s

opctl is a scaffolding tool for creating transformation job specs
`

var (
	disableColoredOut = false

	// colored print
	coloredNotice  = fmt.Sprint
	coloredError   = fmt.Sprint
	coloredSuccess = fmt.Sprint

	OptimusDialTimeout = time.Second * 2
	ConfigName         = "optimus"
	ConfigExtension    = "yaml"
)

func programPrologue(ver string) string {
	return fmt.Sprintf(prologueContents, ver)
}

// New constructs the 'root' command.
// It houses all other sub commands
func New(
	l logger,
	jobSpecRepo store.JobSpecRepository,
	version string,
	conf config.Opctl,
	scheduler models.SchedulerUnit,
) *cli.Command {

	var programName = "opctl"
	var cmd = &cli.Command{
		Use:  programName,
		Long: programPrologue(version),
		PersistentPreRun: func(cmd *cli.Command, args []string) {
			//initialise color if not requested to be disabled
			if !disableColoredOut {
				coloredNotice = color.New(color.Bold, color.FgCyan).SprintFunc()
				coloredError = color.New(color.Bold, color.FgHiRed).SprintFunc()
				coloredSuccess = color.New(color.Bold, color.FgHiGreen).SprintFunc()
			}
		},
	}

	cmd.PersistentFlags().BoolVar(&disableColoredOut, "no-color", disableColoredOut, "disable colored output")

	cmd.AddCommand(createCommand(l, jobSpecRepo, conf))
	cmd.AddCommand(versionCommand(l, version, conf))
	cmd.AddCommand(deployCommand(l, jobSpecRepo, conf))
	cmd.AddCommand(renderCommand(l, conf, jobSpecRepo))
	cmd.AddCommand(configCommand(l))

	// admin specific commands
	switch os.Getenv("OPTIMUS_ADMIN") {
	case "true":
		fallthrough
	case "on":
		fallthrough
	case "1":
		cmd.AddCommand(adminCommand(l))
	}

	return cmd
}

func createConnection(ctx context.Context, host string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	opts = append(opts, grpc.WithBlock())

	conn, err := grpc.DialContext(ctx, host, opts...)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
