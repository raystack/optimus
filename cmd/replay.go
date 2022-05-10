package cmd

import (
	"strings"
	"time"

	cli "github.com/spf13/cobra"

	pb "github.com/odpf/optimus/api/proto/odpf/optimus/core/v1beta1"
	"github.com/odpf/optimus/config"
	"github.com/odpf/optimus/core/set"
)

const (
	replayTimeout = time.Minute * 15
)

type taskRunBlock struct {
	name   string
	height int
	runs   set.Set
}

func taskRunBlockComparator(a, b interface{}) int {
	aAsserted := a.(taskRunBlock)
	bAsserted := b.(taskRunBlock)
	switch {
	case aAsserted.height < bAsserted.height:
		return -1
	case aAsserted.height > bAsserted.height:
		return 1
	}
	return strings.Compare(aAsserted.name, bAsserted.name)
}

// formatRunsPerJobInstance returns a hashmap with Job -> Runs[] mapping
func formatRunsPerJobInstance(instance *pb.ReplayExecutionTreeNode, taskReruns map[string]taskRunBlock, height int) {
	if _, ok := taskReruns[instance.JobName]; !ok {
		taskReruns[instance.JobName] = taskRunBlock{
			name:   instance.JobName,
			height: height,
			runs:   set.NewTreeSetWithTimeComparator(),
		}
	}

	for _, taskRun := range instance.Runs {
		taskReruns[instance.JobName].runs.Add(taskRun.AsTime())
	}
	for _, child := range instance.Dependents {
		formatRunsPerJobInstance(child, taskReruns, height+1)
	}
}

func replayCommand(rootCmd *cli.Command) *cli.Command {
	var (
		configFilePath string
		conf           config.ClientConfig
	)

	cmd := &cli.Command{
		Use:   "replay",
		Short: "Re-running jobs in order to update data for older dates/partitions",
		Long:  `Backfill etl job and all of its downstream dependencies`,
		Annotations: map[string]string{
			"group:core": "true",
		},
	}

	cmd.PersistentFlags().StringVarP(&configFilePath, "config", "c", configFilePath, "File path for client configuration")

	cmd.PersistentPreRunE = func(cmd *cli.Command, args []string) error {
		rootCmd.PersistentPreRun(cmd, args)

		// TODO: find a way to load the config in one place
		c, err := config.LoadClientConfig(configFilePath, cmd.Flags())
		if err != nil {
			return err
		}
		conf = *c

		return nil
	}

	cmd.AddCommand(replayCreateCommand(&conf))
	cmd.AddCommand(replayStatusCommand(&conf))
	cmd.AddCommand(replayListCommand(&conf))
	return cmd
}
