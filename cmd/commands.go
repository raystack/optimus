package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/afero"

	"github.com/odpf/optimus/store"

	"github.com/odpf/optimus/store/local"

	"google.golang.org/grpc"

	"github.com/fatih/color"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	cli "github.com/spf13/cobra"
)

var prologueContents = `optimus %s

optimus is a scaffolding tool for creating transformation job specs
`

var (
	disableColoredOut = false

	// colored print
	coloredNotice  = fmt.Sprint
	coloredError   = fmt.Sprint
	coloredSuccess = fmt.Sprint

	GRPCMaxClientSendSize = 25 << 20 // 25MB
	GRPCMaxClientRecvSize = 25 << 20 // 25MB

	OptimusDialTimeout = time.Second * 2
)

func programPrologue(ver string) string {
	return fmt.Sprintf(prologueContents, ver)
}

// New constructs the 'root' command.
// It houses all other sub commands
func New(
	l logger,
	conf config.Provider,
	tfRepo models.TaskPluginRepository,
	hookRepo models.HookRepo,
	dsRepo models.DatastoreRepo,
) *cli.Command {
	var cmd = &cli.Command{
		Use:  "optimus",
		Long: programPrologue(config.Version),
		PersistentPreRun: func(cmd *cli.Command, args []string) {
			//initialise color if not requested to be disabled
			if !disableColoredOut {
				coloredNotice = color.New(color.Bold, color.FgCyan).SprintFunc()
				coloredError = color.New(color.Bold, color.FgHiRed).SprintFunc()
				coloredSuccess = color.New(color.Bold, color.FgHiGreen).SprintFunc()
			}
		},
		SilenceUsage: true,
	}
	cmd.PersistentFlags().BoolVar(&disableColoredOut, "no-color", disableColoredOut, "disable colored output")

	//init local specs
	var jobSpecRepo store.JobSpecRepository
	if conf.GetJob().Path != "" {
		jobSpecRepo = local.NewJobSpecRepository(
			afero.NewBasePathFs(afero.NewOsFs(), conf.GetJob().Path),
			local.NewJobSpecAdapter(models.TaskRegistry, models.HookRegistry),
		)
	}
	datastoreSpecsFs := map[string]afero.Fs{}
	for _, dsConfig := range conf.GetDatastore() {
		datastoreSpecsFs[dsConfig.Type] = afero.NewBasePathFs(afero.NewOsFs(), dsConfig.Path)
	}

	cmd.AddCommand(versionCommand(l, conf.GetHost()))
	cmd.AddCommand(configCommand(l, dsRepo))
	cmd.AddCommand(createCommand(l, jobSpecRepo, tfRepo, hookRepo, dsRepo, datastoreSpecsFs))
	cmd.AddCommand(deployCommand(l, jobSpecRepo, conf, dsRepo, datastoreSpecsFs))
	cmd.AddCommand(renderCommand(l, conf.GetHost(), jobSpecRepo))
	cmd.AddCommand(validateCommand(l, conf.GetHost(), jobSpecRepo))
	cmd.AddCommand(optimusServeCommand(l, conf))
	cmd.AddCommand(replayCommand(l, conf))

	// admin specific commands
	if conf.GetAdmin().Enabled {
		cmd.AddCommand(adminCommand(l, tfRepo, hookRepo))
	}

	return cmd
}

func createConnection(ctx context.Context, host string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(GRPCMaxClientSendSize),
			grpc.MaxCallRecvMsgSize(GRPCMaxClientRecvSize),
		),
	)

	conn, err := grpc.DialContext(ctx, host, opts...)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
