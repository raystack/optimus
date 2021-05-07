package bq2bq

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mitchellh/hashstructure/v2"
	"github.com/patrickmn/go-cache"
	"github.com/spf13/cast"

	"cloud.google.com/go/bigquery"
	"github.com/AlecAivazis/survey/v2"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/utils"
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

	QueryFileName = "query.sql"

	// Required secret
	SecretName = "TASK_BQ2BQ"

	TimeoutDuration = time.Second * 120
	MaxBQApiRetries = 3
	FakeSelectStmt  = "SELECT * from `%s` WHERE FALSE LIMIT 1"

	CacheTTL         = time.Minute * 60
	CacheCleanUp     = time.Minute * 30
	ErrCacheNotFound = errors.New("item not found")
)

type ClientFactory interface {
	New(ctx context.Context, svcAccount string) (bqiface.Client, error)
}

type BQ2BQ struct {
	ClientFac ClientFactory
	mu        sync.Mutex
	C         *cache.Cache
}

func (b *BQ2BQ) Name() string {
	return "bq2bq"
}

func (b *BQ2BQ) Description() string {
	return "BigQuery to BigQuery transformation task"
}

func (b *BQ2BQ) Image() string {
	return "odpf/optimus-task-bq2bq:0.0.1"
}

func (b *BQ2BQ) AskQuestions(_ models.AskQuestionRequest) (models.AskQuestionResponse, error) {
	questions := []*survey.Question{
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
				Options: []string{"REPLACE", "MERGE", "APPEND"},
				Default: "MERGE",
				Help: `
REPLACE - Deletes existing partition and insert result of select query
MERGE   - DML statements, BQ scripts
APPEND  - Append to existing table
`,
			},
			Validate: survey.Required,
		},
	}
	inputsRaw := make(map[string]interface{})
	if err := survey.Ask(questions, &inputsRaw); err != nil {
		return models.AskQuestionResponse{}, err
	}

	if load, ok := inputsRaw["LoadMethod"]; ok && load.(survey.OptionAnswer).Value == "REPLACE" {
		filterExp := ""
		if err := survey.AskOne(&survey.Input{
			Message: "Partition filter expression",
			Default: "",
			Help: `Where condition over partitioned column used to delete existing partitions
in destination table. These partitions will be replaced with sql query result.
Leave empty for optimus to automatically figure this out although it will be 
faster and cheaper to provide the exact condition.
for example: DATE(event_timestamp) >= "{{ .DSTART|Date }}" AND DATE(event_timestamp) < "{{ .DEND|Date }}"`,
		}, &filterExp); err != nil {
			return models.AskQuestionResponse{}, err
		}
		inputsRaw["Filter"] = filterExp
	}
	return models.AskQuestionResponse{
		Answers: inputsRaw,
	}, nil
}

func (b *BQ2BQ) GenerateConfig(request models.GenerateConfigRequest) (models.GenerateConfigResponse, error) {
	stringInputs, err := utils.ConvertToStringMap(request.Inputs)
	if err != nil {
		return models.GenerateConfigResponse{}, nil
	}
	conf := models.JobSpecConfigs{
		{
			Name:  "PROJECT",
			Value: stringInputs["Project"],
		},
		{
			Name:  "TABLE",
			Value: stringInputs["Table"],
		},
		{
			Name:  "DATASET",
			Value: stringInputs["Dataset"],
		},
		{
			Name:  "LOAD_METHOD",
			Value: stringInputs["LoadMethod"],
		},
		{
			Name:  "SQL_TYPE",
			Value: "STANDARD",
		},
	}
	if _, ok := stringInputs["Filter"]; ok {
		conf = append(conf, models.JobSpecConfigItem{
			Name:  "PARTITION_FILTER",
			Value: stringInputs["Filter"],
		})
	}
	return models.GenerateConfigResponse{
		Config: conf,
	}, nil
}

func (b *BQ2BQ) GenerateAssets(_ models.GenerateAssetsRequest) (models.GenerateAssetsResponse, error) {
	return models.GenerateAssetsResponse{
		Assets: map[string]string{
			QueryFileName: `-- SQL query goes here

Select * from "project.dataset.table";
`,
		},
	}, nil
}

// GenerateDestination uses config details to build target table
// this format should match with GenerateDependencies output
func (b *BQ2BQ) GenerateDestination(request models.GenerateDestinationRequest) (models.GenerateDestinationResponse, error) {
	proj, ok1 := request.Config.Get("PROJECT")
	dataset, ok2 := request.Config.Get("DATASET")
	tab, ok3 := request.Config.Get("TABLE")
	if ok1 && ok2 && ok3 {
		return models.GenerateDestinationResponse{
			Destination: fmt.Sprintf("%s:%s.%s", proj, dataset, tab),
		}, nil
	}
	return models.GenerateDestinationResponse{}, errors.New("missing config key required to generate destination")
}

// GenerateDependencies uses assets to find out the source tables of this
// transformation.
// Try using BQ APIs to search for referenced tables. This work for Select stmts
// but not for Merge/Scripts, for them use regex based search and then create
// fake select stmts. Fake statements help finding actual referenced tables in
// case regex based table is a view & not actually a source table. Because this
// fn should generate the actual source as dependency
// BQ2BQ dependencies are BQ tables in format "project:dataset.table"
func (b *BQ2BQ) GenerateDependencies(request models.GenerateDependenciesRequest) (response models.GenerateDependenciesResponse, err error) {
	response.Dependencies = []string{}

	// check if exists in cache
	if cachedResponse, err := b.IsCached(request); err == nil {
		// cache ready
		return cachedResponse, nil
	} else if err != ErrCacheNotFound {
		return models.GenerateDependenciesResponse{}, err
	}

	// TODO(kush.sharma): should we ask context as input? Not sure if its okay
	// for the task to allow handling there own timeouts/deadlines
	timeoutCtx, cancel := context.WithTimeout(context.Background(), TimeoutDuration)
	defer cancel()

	svcAcc, ok := request.Project.Secret.GetByName(SecretName)
	if !ok || len(svcAcc) == 0 {
		return response, errors.New(fmt.Sprintf("secret %s required to generate dependencies not found for %s", SecretName, b.Name()))
	}

	// try to resolve referenced tables directly from BQ APIs
	response.Dependencies, err = b.FindDependenciesWithRetryableDryRun(timeoutCtx, request.Assets[QueryFileName], svcAcc)
	if err != nil {
		return response, err
	}

	if len(response.Dependencies) == 0 {
		// could be BQ script, find table names using regex and create
		// fake Select STMTs to find actual referenced tables
		parsedDependencies, err := b.FindDependenciesWithRegex(request)
		if err != nil {
			return response, err
		}

		resultChan := make(chan []string)
		eg, apiCtx := errgroup.WithContext(timeoutCtx) // it will stop executing further after first error
		for _, tableName := range parsedDependencies {
			fakeQuery := fmt.Sprintf(FakeSelectStmt, tableName)
			// find dependencies in parallel
			eg.Go(func() error {
				//prepare dummy query
				deps, err := b.FindDependenciesWithRetryableDryRun(timeoutCtx, fakeQuery, svcAcc)
				if err != nil {
					return err
				}
				select {
				case resultChan <- deps:
					return nil
				// timeoutCtx requests to be cancelled
				case <-apiCtx.Done():
					return apiCtx.Err()
				}
			})
		}

		go func() {
			// if all done, stop waiting for results
			eg.Wait()
			close(resultChan)
		}()

		// accumulate results
		for dep := range resultChan {
			response.Dependencies = append(response.Dependencies, dep...)
		}

		// check if wait was finished because of an error
		if err := eg.Wait(); err != nil {
			return response, err
		}
	}

	// before returning remove self
	selfTable, err := b.GenerateDestination(models.GenerateDestinationRequest{
		Config:  request.Config,
		Assets:  request.Assets,
		Project: request.Project,
	})
	if err != nil {
		return response, err
	}
	response.Dependencies = removeString(response.Dependencies, selfTable.Destination)
	b.Cache(request, response)

	return response, nil
}

// FindDependenciesWithRegex look for table patterns in SQL query to find
// source tables.
// Config is required to generate destination and avoid cycles
func (b *BQ2BQ) FindDependenciesWithRegex(request models.GenerateDependenciesRequest) ([]string, error) {
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

	queryString := request.Assets[QueryFileName]
	tablesFound := make(map[string]bool)
	pseudoTables := make(map[string]bool)

	// we mark destination as a pseudo table to avoid a dependency
	// cycle. This is for supporting DML queries that may also refer
	// to themselves.
	dest, err := b.GenerateDestination(models.GenerateDestinationRequest{
		Config:  request.Config,
		Assets:  request.Assets,
		Project: request.Project,
	})
	if err != nil {
		return nil, err
	}
	pseudoTables[dest.Destination] = true

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

func (b *BQ2BQ) FindDependenciesWithRetryableDryRun(ctx context.Context, query, svcAccSecret string) ([]string, error) {
	for try := 1; try <= MaxBQApiRetries; try++ {
		client, err := b.ClientFac.New(ctx, svcAccSecret)
		if err != nil {
			return nil, errors.New("bigquery client")
		}
		deps, err := b.FindDependenciesWithDryRun(ctx, client, query)
		if err != nil {
			if strings.Contains(err.Error(), "net/http: TLS handshake timeout") ||
				strings.Contains(err.Error(), "unexpected EOF") ||
				strings.Contains(err.Error(), "i/o timeout") ||
				strings.Contains(err.Error(), "connection reset by peer") {
				// retry
				continue
			}

			return nil, err
		}
		return deps, nil
	}
	return nil, errors.New("bigquery api retries exhausted")
}

func (b *BQ2BQ) FindDependenciesWithDryRun(ctx context.Context, client bqiface.Client, query string) ([]string, error) {
	q := client.Query(query)
	q.SetQueryConfig(bqiface.QueryConfig{
		QueryConfig: bigquery.QueryConfig{
			Q:      query,
			DryRun: true,
		},
	})

	job, err := q.Run(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "query run")
	}
	// Dry run is not asynchronous, so get the latest status and statistics.
	status := job.LastStatus()
	if err := status.Err(); err != nil {
		return nil, errors.Wrap(err, "query status")
	}

	details, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok {
		return nil, errors.New("failed to cast to Query Statistics")
	}

	tables := []string{}
	for _, tab := range details.ReferencedTables {
		tables = append(tables, tab.FullyQualifiedName())
	}
	return tables, nil
}

func createTableName(proj, dataset, table string) string {
	return fmt.Sprintf("%s.%s.%s", proj, dataset, table)
}

func deduplicateStrings(in []string) []string {
	if len(in) == 0 {
		return in
	}

	sort.Strings(in)
	j := 0
	for i := 1; i < len(in); i++ {
		if in[j] == in[i] {
			continue
		}
		j++
		// preserve the original data
		// in[i], in[j] = in[j], in[i]
		// only set what is required
		in[j] = in[i]
	}
	return in[:j+1]
}

func removeString(s []string, match string) []string {
	if len(s) == 0 {
		return s
	}
	idx := -1
	for i, tab := range s {
		if tab == match {
			idx = i
			break
		}
	}
	// not found
	if idx < 0 {
		return s
	}
	s[len(s)-1], s[idx] = s[idx], s[len(s)-1]
	return s[:len(s)-1]
}

func (b *BQ2BQ) IsCached(request models.GenerateDependenciesRequest) (models.GenerateDependenciesResponse, error) {
	if b.C == nil {
		return models.GenerateDependenciesResponse{}, ErrCacheNotFound
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	requestHash, err := hashstructure.Hash(request, hashstructure.FormatV2, nil)
	if err != nil {
		return models.GenerateDependenciesResponse{}, err
	}
	hashString := cast.ToString(requestHash)
	if item, ok := b.C.Get(hashString); ok {
		return item.(models.GenerateDependenciesResponse), nil
	}
	return models.GenerateDependenciesResponse{}, ErrCacheNotFound
}

func (b *BQ2BQ) Cache(request models.GenerateDependenciesRequest, response models.GenerateDependenciesResponse) error {
	if b.C == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	requestHash, err := hashstructure.Hash(request, hashstructure.FormatV2, nil)
	if err != nil {
		return err
	}
	hashString := cast.ToString(requestHash)
	b.C.Set(hashString, response, cache.DefaultExpiration)
	return nil
}

func init() {
	if err := models.TaskRegistry.Add(&BQ2BQ{
		ClientFac: &defaultBQClientFactory{},
		C:         cache.New(CacheTTL, CacheCleanUp),
	}); err != nil {
		panic(err)
	}
}
