package bq2bq

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/AlecAivazis/survey/v2"
	"github.com/odpf/optimus/models"
)

var (
	validateName = survey.ComposeValidators(
		validatorFactory.NewFromRegex(`^[a-zA-Z0-9_\-]+$`, `invalid name (can only contain characters A-Z (in either case), 0-9, "-" or "_")`),
		survey.MinLength(3),
	)
	// a big query table can only contain the the characters [a-zA-Z0-9_].
	// https://cloud.google.com/bigquery/docs/tables
	validateTableName = survey.ComposeValidators(
		validatorFactory.NewFromRegex(`^[a-zA-Z0-9_-]+$`, "invalid table name (can only contain characters A-Z (in either case), 0-9, hyphen(-) or underscore (_)"),
		survey.MaxLength(1024),
		survey.MinLength(3),
	)

	tableDestinationPatterns = regexp.MustCompile("" +
		"(?i)(?:FROM)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.(\\w+)`?" +
		"|" +
		"(?i)(?:JOIN)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.(\\w+)`?" +
		"|" +
		"(?i)(?:WITH)\\s*(?:/\\*\\s*([a-zA-Z0-9@_-]*)\\s*\\*/)?\\s+`?([\\w-]+)\\.([\\w-]+)\\.(\\w+)`?\\s+(?:AS)")

	queryCommentPatterns = regexp.MustCompile("(--.*)|(((/\\*)+?[\\w\\W]*?(\\*/)+))")
	helperPattern        = regexp.MustCompile("(\\/\\*\\s*(@[a-zA-Z0-9_-]+)\\s*\\*\\/)")

	queryFileName = "query.sql"
)

type BQ2BQ struct {
}

func (b *BQ2BQ) GetName() string {
	return "bq2bq"
}

func (b *BQ2BQ) GetDescription() string {
	return "BigQuery to BigQuery transformation task"
}

func (b *BQ2BQ) GetImage() string {
	return "odpf/de-bumblebee:e63c6e53f1f011477301a3b0bfe4f4372528ba77"
}

func (b *BQ2BQ) GetQuestions() []*survey.Question {
	return []*survey.Question{
		{
			Name:     "Project",
			Prompt:   &survey.Input{Message: "Project ID:"},
			Validate: validateName,
		},
		{
			Name:     "Dataset",
			Prompt:   &survey.Input{Message: "Dataset Name:"},
			Validate: validateName,
		},
		{
			Name:     "Table",
			Prompt:   &survey.Input{Message: "Table Name:"},
			Validate: validateTableName,
		},
		{
			Name: "LoadMethod",
			Prompt: &survey.Select{
				Message: "Load method to use on destination?",
				Options: []string{"REPLACE", "APPEND", "MERGE"},
				Default: "MERGE",
			},
			Validate: survey.Required,
		},
	}
}

func (b *BQ2BQ) GetConfig() map[string]string {
	return map[string]string{
		"project":     "{{.Project}}",
		"dataset":     "{{.Dataset}}",
		"table":       "{{.Table}}",
		"load_method": "{{.LoadMethod}}",
		"sql_type":    "STANDARD",
	}
}

func (b *BQ2BQ) GetAssets() map[string]string {
	return map[string]string{
		queryFileName: `Select * from "project.dataset.table"`,
	}
}

func (b *BQ2BQ) GenerateDestination(data models.UnitData) (string, error) {
	// check if configs exists
	// .. TODO
	return fmt.Sprintf("%s.%s.%s", data.Config["project"], data.Config["dataset"], data.Config["table"]), nil
}

func (b *BQ2BQ) GenerateDependencies(data models.UnitData) ([]string, error) {
	// we look for certain patterns in the query source code
	// in particular, we look for the following constructs
	// * from {table} ...
	// * join {table} ...
	// * with {table} as ...
	// where {table} => {project}.{dataset}.{name}
	// for `from` and `join` we build a optimus.Table object and
	// store it's name in a set. For `with` query we store the name in
	// a separate set called `pseudoTables` that is used for filtering
	// out tables from `from`/`join` matches.
	// the algorithm roughly locates all from/join clauses, filters it
	// in case it's a known pseudo table (since with queries come before
	// either `from` or `join` queries, so they're match first).
	// notice that only clauses that end in "." delimited sequences
	// are matched (for instance: foo.bar.baz, but not foo.bar).
	// This helps weed out pseudo tables since most of the time
	// they're a single sequence of characters. But on the other hand
	// this also means that otherwise valid reference to "dataset.table"
	// will not be recognised.

	queryString := data.Assets[queryFileName]
	tablesFound := make(map[string]bool)
	pseudoTables := make(map[string]bool)

	// remove comments from query
	matches := queryCommentPatterns.FindAllStringSubmatch(queryString, -1)
	for _, match := range matches {
		helperToken := match[2]

		// check if its a helper
		if helperPattern.MatchString(helperToken) {
			continue
		}

		// replace full match
		queryString = strings.ReplaceAll(queryString, match[0], " ")
	}

	// we mark destination as a pseudo table to avoid a dependency
	// cycle. This is for supporting DML queries that may also refer
	// to themselves.
	dest, err := b.GenerateDestination(data)
	if err != nil {
		return nil, err
	}
	pseudoTables[dest] = true

	matches = tableDestinationPatterns.FindAllStringSubmatch(queryString, -1)
	for _, match := range matches {
		var projectIdx, datasetIdx, nameIdx, ignoreUpstreamIdx int
		tokens := strings.Fields(match[0])
		clause := strings.ToLower(tokens[0])

		switch clause {
		case "from":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 1, 2, 3, 4
		case "join":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 5, 6, 7, 8
		case "with":
			ignoreUpstreamIdx, projectIdx, datasetIdx, nameIdx = 9, 10, 11, 12
		}

		// if upstream is ignored, don't treat it as source
		if strings.TrimSpace(match[ignoreUpstreamIdx]) == "@ignoreupstream" {
			continue
		}

		tableName := createTableName(match[projectIdx], match[datasetIdx], match[nameIdx])
		if clause == "with" {
			pseudoTables[tableName] = true
		} else {
			tablesFound[tableName] = true
		}
	}
	var tables []string
	for table := range tablesFound {
		if pseudoTables[table] {
			continue
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func createTableName(proj, dataset, table string) string {
	return fmt.Sprintf("%s.%s.%s", proj, dataset, table)
}

func init() {
	models.TaskRegistry.Add(&BQ2BQ{})
}
