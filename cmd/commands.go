package cmd

import (
	"errors"
	"fmt"
	"os"

	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/salt/cmdx"
	"github.com/odpf/salt/term"
	cli "github.com/spf13/cobra"
)

var (
	disableColoredOut = false
	// colored print
	coloredNotice  = fmt.Sprintf
	coloredError   = fmt.Sprintf
	coloredSuccess = fmt.Sprintf

	ErrServerNotReachable = func(host string) error {
		return errors.New(heredoc.Docf(`Unable to reach optimus server at %s, this can happen due to following reasons:
			1. Check if you are connected to internet
			2. Is the host correctly configured in optimus config
			3. Is Optimus server currently unreachable`, host))
	}
)

// New constructs the 'root' command. It houses all other sub commands
// default output of logging should go to stdout
// interactive output like progress bars should go to stderr
// unless the stdout/err is a tty, colors/progressbar should be disabled
func New() *cli.Command {
	disableColoredOut = !isTerminal(os.Stdout)

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
		PersistentPreRun: func(cmd *cli.Command, args []string) {
			// initialise color if not requested to be disabled
			cs := term.NewColorScheme()
			if !disableColoredOut {
				coloredNotice = func(s string, a ...interface{}) string {
					return cs.Yellowf(s, a...)
				}
				coloredError = func(s string, a ...interface{}) string {
					return cs.Redf(s, a...)
				}
				coloredSuccess = func(s string, a ...interface{}) string {
					return cs.Greenf(s, a...)
				}
			}
		},
	}

	cmdx.SetHelp(cmd)
	cmd.PersistentFlags().BoolVar(&disableColoredOut, "no-color", disableColoredOut, "Disable colored output")

	cmd.AddCommand(adminCommand(cmd))
	cmd.AddCommand(backupCommand(cmd))
	cmd.AddCommand(deployCommand())
	cmd.AddCommand(initCommand())
	cmd.AddCommand(jobCommand(cmd))
	cmd.AddCommand(namespaceCommand())
	cmd.AddCommand(projectCommand())
	cmd.AddCommand(replayCommand(cmd))
	cmd.AddCommand(resourceCommand(cmd))
	cmd.AddCommand(secretCommand(cmd))
	cmd.AddCommand(versionCommand())

	cmd.AddCommand(serveCommand())

	addExtensionCommand(cmd)
	return cmd
}
