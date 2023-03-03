package extension

import (
	"bytes"
	"fmt"

	"github.com/goto/salt/log"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	"github.com/goto/optimus/client/extension/model"
)

type describeCommand struct {
	logger  log.Logger
	project *model.RepositoryProject
}

func newDescribeCommand(logger log.Logger, project *model.RepositoryProject) *cobra.Command {
	describe := &describeCommand{
		logger:  logger,
		project: project,
	}

	cmd := &cobra.Command{
		Use:   "describe",
		Short: "describe is a sub command to allow user to describe extension",
		RunE:  describe.RunE,
	}
	return cmd
}

func (d *describeCommand) RunE(cmd *cobra.Command, _ []string) error {
	verbose, _ := cmd.Flags().GetBool("verbose")

	header := d.buildHeader()
	var table string
	if verbose {
		table = d.buildVerboseTable()
	} else {
		table = d.buildSimpleTable()
	}
	content := header + "\nRelease:\n" + table
	d.logger.Info(content)
	return nil
}

func (d *describeCommand) buildHeader() string {
	var output string
	output += fmt.Sprintf("Onwer name: %s\n", d.project.Owner.Name)
	output += fmt.Sprintf("Project name: %s\n", d.project.Name)
	output += fmt.Sprintf("Command name: %s\n", d.project.CommandName)
	return output
}

func (d *describeCommand) buildVerboseTable() string {
	buff := &bytes.Buffer{}
	table := tablewriter.NewWriter(buff)
	table.SetHeader([]string{"tag", "active", "current api path", "upgrade api path"})
	for _, release := range d.project.Releases {
		var active string
		if release.TagName == d.project.ActiveTagName {
			active = "true"
		}
		table.Append([]string{release.TagName, active, release.CurrentAPIPath, release.UpgradeAPIPath})
	}
	table.Render()
	return buff.String()
}

func (d *describeCommand) buildSimpleTable() string {
	buff := &bytes.Buffer{}
	table := tablewriter.NewWriter(buff)
	table.SetHeader([]string{"tag", "active"})
	for _, release := range d.project.Releases {
		var active string
		if release.TagName == d.project.ActiveTagName {
			active = "true"
		}
		table.Append([]string{release.TagName, active})
	}
	table.Render()
	return buff.String()
}
