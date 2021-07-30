package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/afero"

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

	GRPCMaxClientSendSize = 45 << 20 // 45MB
	GRPCMaxClientRecvSize = 45 << 20 // 45MB

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

// New constructs the 'root' command.
// It houses all other sub commands
func New(
	l logger,
	conf config.Provider,
	pluginRepo models.PluginRepository,
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

	cmd.AddCommand(versionCommand(l, conf.GetHost(), pluginRepo))
	cmd.AddCommand(configCommand(l, dsRepo))
	cmd.AddCommand(createCommand(l, jobSpecFs, datastoreSpecsFs, pluginRepo, dsRepo))
	cmd.AddCommand(deployCommand(l, conf, jobSpecRepo, pluginRepo, dsRepo, datastoreSpecsFs))
	cmd.AddCommand(renderCommand(l, conf.GetHost(), jobSpecRepo))
	cmd.AddCommand(validateCommand(l, conf.GetHost(), pluginRepo, jobSpecRepo))
	cmd.AddCommand(optimusServeCommand(l, conf))
	cmd.AddCommand(replayCommand(l, conf))

	// admin specific commands
	if conf.GetAdmin().Enabled {
		cmd.AddCommand(adminCommand(l, pluginRepo))
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
