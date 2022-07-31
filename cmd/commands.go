package cmd

import (
	"os"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/salt/cmdx"
	"github.com/odpf/salt/term"
	cli "github.com/spf13/cobra"

	"github.com/odpf/optimus/cmd/admin"
	"github.com/odpf/optimus/cmd/backup"
	"github.com/odpf/optimus/cmd/deploy"
	"github.com/odpf/optimus/cmd/extension"
	"github.com/odpf/optimus/cmd/initialize"
	"github.com/odpf/optimus/cmd/job"
	"github.com/odpf/optimus/cmd/logger"
	"github.com/odpf/optimus/cmd/namespace"
	"github.com/odpf/optimus/cmd/plugin"
	"github.com/odpf/optimus/cmd/project"
	"github.com/odpf/optimus/cmd/replay"
	"github.com/odpf/optimus/cmd/resource"
	"github.com/odpf/optimus/cmd/secret"
	"github.com/odpf/optimus/cmd/serve"
	"github.com/odpf/optimus/cmd/version"
	"github.com/odpf/optimus/utils"
)

// New constructs the 'root' command. It houses all other sub commands
// default output of logging should go to stdout
// interactive output like progress bars should go to stderr
// unless the stdout/err is a tty, colors/progressbar should be disabled
func New() *cli.Command {
	if utils.IsTerminal(os.Stdout) {
		initializeColor()
	}

	cmd := &cli.Command{
		Use: "optimus <command> <subcommand> [flags]",
		Long: heredoc.Doc(`
			Optimus is an easy-to-use, reliable, and performant workflow orchestrator for 
			data transformation, data modeling, pipelines, and data quality management.
		
			For passing authentication header, set one of the following environment
			variables:
			1. OPTIMUS_AUTH_BASIC_TOKEN
			2. OPTIMUS_AUTH_BEARER_TOKEN`),
		SilenceUsage: true,
		Example: heredoc.Doc(`
				$ optimus job create
				$ optimus backup create
				$ optimus backup list
				$ optimus replay create
			`),
		Annotations: map[string]string{
			"group:core": "true",
			"help:learn": heredoc.Doc(`
				Use 'optimus <command> <subcommand> --help' for more information about a command.
				Read the manual at https://odpf.github.io/optimus/
			`),
			"help:feedback": heredoc.Doc(`
				Open an issue here https://github.com/odpf/optimus/issues
			`),
		},
	}

	cmdx.SetHelp(cmd)

	// Client related commands
	cmd.AddCommand(
		admin.NewAdminCommand(),
		backup.NewBackupCommand(),
		deploy.NewDeployCommand(),
		initialize.NewInitializeCommand(),
		job.NewJobCommand(),
		namespace.NewNamespaceCommand(),
		project.NewProjectCommand(),
		replay.NewReplayCommand(),
		resource.NewResourceCommand(),
		secret.NewSecretCommand(),
		version.NewVersionCommand(),
	)
	// Server related commands
	cmd.AddCommand(
		serve.NewServeCommand(),
		plugin.NewPluginCommand(),
	)

	extension.UpdateWithExtension(cmd)
	return cmd
}

func initializeColor() {
	cs := term.NewColorScheme()
	logger.ColoredNotice = func(s string, a ...interface{}) string {
		return cs.Yellowf(s, a...)
	}
	logger.ColoredError = func(s string, a ...interface{}) string {
		return cs.Redf(s, a...)
	}
	logger.ColoredSuccess = func(s string, a ...interface{}) string {
		return cs.Greenf(s, a...)
	}
}
