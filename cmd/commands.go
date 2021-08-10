package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/fatih/color"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
	"github.com/odpf/salt/log"
	"github.com/spf13/afero"
	cli "github.com/spf13/cobra"
	"google.golang.org/grpc"
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
	coloredShow    = fmt.Sprint
	coloredPrint   = fmt.Sprint

	GRPCMaxClientSendSize      = 45 << 20 // 45MB
	GRPCMaxClientRecvSize      = 45 << 20 // 45MB
	GRPCMaxRetry          uint = 3

	OptimusDialTimeout = time.Second * 2
)

func programPrologue(ver string) string {
	return fmt.Sprintf(prologueContents, ver)
}

// JobSpecRepository represents a storage interface for Job specifications locally
type JobSpecRepository interface {
	SaveAt(models.JobSpec, string) error
	Save(models.JobSpec) error
	GetByName(string) (models.JobSpec, error)
	GetAll() ([]models.JobSpec, error)
}

// New constructs the 'root' command.It houses all other sub commands
func New(plainLog log.Logger, jsonLog log.Logger, conf config.Provider, pluginRepo models.PluginRepository, dsRepo models.DatastoreRepo) *cli.Command {
	var cmd = &cli.Command{
		Use:  "optimus",
		Long: programPrologue(config.Version),
		PersistentPreRun: func(cmd *cli.Command, args []string) {
			//initialise color if not requested to be disabled
			if !disableColoredOut {
				coloredNotice = color.New(color.Bold, color.FgCyan).SprintFunc()
				coloredError = color.New(color.Bold, color.FgHiRed).SprintFunc()
				coloredSuccess = color.New(color.Bold, color.FgHiGreen).SprintFunc()
				coloredShow = color.New(color.Bold, color.FgHiWhite).SprintFunc()
				coloredPrint = color.New(color.Bold, color.FgHiYellow).SprintFunc()
			}
		},
		SilenceUsage: true,
	}
	cmd.PersistentFlags().BoolVar(&disableColoredOut, "no-color", disableColoredOut, "disable colored output")

	//init local specs
	var jobSpecRepo JobSpecRepository
	jobSpecFs := afero.NewBasePathFs(afero.NewOsFs(), conf.GetJob().Path)
	if conf.GetJob().Path != "" {
		jobSpecRepo = local.NewJobSpecRepository(
			jobSpecFs,
			local.NewJobSpecAdapter(pluginRepo),
		)
	}
	datastoreSpecsFs := map[string]afero.Fs{}
	for _, dsConfig := range conf.GetDatastore() {
		datastoreSpecsFs[dsConfig.Type] = afero.NewBasePathFs(afero.NewOsFs(), dsConfig.Path)
	}

	cmd.AddCommand(versionCommand(plainLog, conf.GetHost(), pluginRepo))
	cmd.AddCommand(configCommand(plainLog, dsRepo))
	cmd.AddCommand(createCommand(plainLog, jobSpecFs, datastoreSpecsFs, pluginRepo, dsRepo))
	cmd.AddCommand(deployCommand(plainLog, conf, jobSpecRepo, pluginRepo, dsRepo, datastoreSpecsFs))
	cmd.AddCommand(renderCommand(plainLog, conf.GetHost(), jobSpecRepo))
	cmd.AddCommand(validateCommand(plainLog, conf.GetHost(), pluginRepo, jobSpecRepo))
	cmd.AddCommand(serveCommand(jsonLog, conf))
	cmd.AddCommand(replayCommand(plainLog, conf))
	cmd.AddCommand(runCommand(plainLog, conf.GetHost(), jobSpecRepo, pluginRepo))

	// admin specific commands
	if conf.GetAdmin().Enabled {
		cmd.AddCommand(adminCommand(plainLog, pluginRepo))
	}

	return cmd
}

func createConnection(ctx context.Context, host string) (*grpc.ClientConn, error) {
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(100 * time.Millisecond)),
		grpc_retry.WithMax(GRPCMaxRetry),
	}
	var opts []grpc.DialOption
	opts = append(opts,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(GRPCMaxClientSendSize),
			grpc.MaxCallRecvMsgSize(GRPCMaxClientRecvSize),
		),
		grpc.WithUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...)),
	)

	return grpc.DialContext(ctx, host, opts...)
}
