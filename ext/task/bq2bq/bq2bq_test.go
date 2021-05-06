package bq2bq_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/odpf/optimus/instance"

	"github.com/patrickmn/go-cache"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/ext/task/bq2bq"
	"github.com/odpf/optimus/models"
)

type bqClientMock struct {
	mock.Mock
	bqiface.Client
}

func (cli *bqClientMock) Location() string {
	panic("not implemented")
}

func (cli *bqClientMock) SetLocation(string) {
	panic("not implemented")
}

func (cli *bqClientMock) Close() error {
	return cli.Called().Error(0)
}

func (cli *bqClientMock) Dataset(dataset string) bqiface.Dataset {
	panic("not implemented")
}

func (cli *bqClientMock) DatasetInProject(project string, dataset string) bqiface.Dataset {
	panic("not implemented")
}

func (cli *bqClientMock) Datasets(context.Context) bqiface.DatasetIterator {
	panic("not implemented")
}

func (cli *bqClientMock) DatasetsInProject(context.Context, string) bqiface.DatasetIterator {
	panic("not implemented")
}

func (cli *bqClientMock) Query(q string) bqiface.Query {
	return cli.Called(q).Get(0).(bqiface.Query)
}

func (cli *bqClientMock) JobFromID(context.Context, string) (bqiface.Job, error) {
	panic("not implemented")
}

func (cli *bqClientMock) JobFromIDLocation(context.Context, string, string) (bqiface.Job, error) {
	panic("not implemented")
}

func (cli *bqClientMock) Jobs(context.Context) bqiface.JobIterator {
	panic("not implemented")
}

func (cli *bqClientMock) embedToIncludeNewMethods() {
	panic("not implemented")
}

type bqJob struct {
	mock.Mock
	bqiface.Job
}

func (j *bqJob) Wait(ctx context.Context) (*bigquery.JobStatus, error) {
	args := j.Called(ctx)
	return args.Get(0).(*bigquery.JobStatus), args.Error(1)
}

func (j *bqJob) ID() string {
	panic("not implemented")
}

func (j *bqJob) Location() string {
	panic("not implemented")
}

func (j *bqJob) Config() (bigquery.JobConfig, error) {
	panic("not implemented")
}

func (j *bqJob) Status(c context.Context) (*bigquery.JobStatus, error) {
	args := j.Called(c)
	return args.Get(0).(*bigquery.JobStatus), args.Error(1)
}

func (j *bqJob) LastStatus() *bigquery.JobStatus {
	return j.Called().Get(0).(*bigquery.JobStatus)
}

func (j *bqJob) Cancel(context.Context) error {
	panic("not implemented")
}

func (j *bqJob) Read(context.Context) (bqiface.RowIterator, error) {
	panic("not implemented")
}

type bqQuery struct {
	mock.Mock
	bqiface.Query
}

func (q *bqQuery) JobIDConfig() *bigquery.JobIDConfig {
	return q.Called().Get(0).(*bigquery.JobIDConfig)
}

func (q *bqQuery) SetQueryConfig(c bqiface.QueryConfig) {
	q.Called(c)
}

func (q *bqQuery) Run(c context.Context) (bqiface.Job, error) {
	args := q.Called(c)
	return args.Get(0).(bqiface.Job), args.Error(1)
}

func (q *bqQuery) Read(c context.Context) (bqiface.RowIterator, error) {
	args := q.Called(c)
	return args.Get(0).(bqiface.RowIterator), args.Error(1)
}

type bqClientFactoryMock struct {
	mock.Mock
}

func (fac *bqClientFactoryMock) New(ctx context.Context, svcAcc string) (bqiface.Client, error) {
	args := fac.Called(ctx, svcAcc)
	return args.Get(0).(bqiface.Client), args.Error(1)
}

func TestBQ2BQ(t *testing.T) {
	t.Run("GenerateDestination", func(t *testing.T) {
		t.Run("should properly generate a destination provided correct config inputs", func(t *testing.T) {
			b2b := &bq2bq.BQ2BQ{}
			dst, err := b2b.GenerateDestination(models.GenerateDestinationRequest{
				Config: models.JobSpecConfigs{
					{
						Name:  "PROJECT",
						Value: "proj",
					},
					{
						Name:  "DATASET",
						Value: "datas",
					},
					{
						Name:  "TABLE",
						Value: "tab",
					},
				},
			})
			assert.Nil(t, err)
			assert.Equal(t, "proj:datas.tab", dst.Destination)
		})
		t.Run("should throw an error if any on of the config is missing to generate destination", func(t *testing.T) {
			b2b := &bq2bq.BQ2BQ{}
			_, err := b2b.GenerateDestination(models.GenerateDestinationRequest{
				Config: models.JobSpecConfigs{
					{
						Name:  "DATASET",
						Value: "datas",
					},
					{
						Name:  "TABLE",
						Value: "tab",
					},
				},
			})
			assert.NotNil(t, err)
		})
	})

	t.Run("CompileAssets", func(t *testing.T) {
		t.Run("should correctly break replace load method with window size greater than 24h", func(t *testing.T) {
			schdAt := time.Date(2021, 5, 6, 2, 3, 0, 0, time.UTC)
			b2b := &bq2bq.BQ2BQ{
				TemplateEngine: instance.NewGoEngine(),
			}
			resp, err := b2b.CompileAssets(models.CompileAssetsRequest{
				TaskWindow: models.JobSpecTaskWindow{
					Size:       time.Hour * 48,
					Offset:     0,
					TruncateTo: "h",
				},
				Config: []models.JobSpecConfigItem{
					{
						Name:  "LOAD_METHOD",
						Value: "REPLACE",
					},
				},
				Assets: map[string]string{
					bq2bq.QueryFileName: "Select * from `proj.datas.table1` WHERE ts < {{.DSTART}} and ts > {{.DEND}}",
				},
				InstanceSchedule: schdAt,
				InstanceData: []models.InstanceSpecData{
					{
						Name:  instance.ConfigKeyExecutionTime,
						Value: schdAt.Format(models.InstanceScheduledAtTimeLayout),
						Type:  models.InstanceDataTypeEnv,
					},
				},
			})
			assert.Nil(t, err)
			assert.Equal(t, resp.Assets[bq2bq.QueryFileName], "Select * from `proj.datas.table1` WHERE ts < 2021-05-04T02:00:00Z and ts > 2021-05-05T02:00:00Z"+
				bq2bq.QueryFileReplaceBreakMarker+
				"Select * from `proj.datas.table1` WHERE ts < 2021-05-05T02:00:00Z and ts > 2021-05-06T02:00:00Z")
		})
	})

	t.Run("FindDependenciesWithRegex", func(t *testing.T) {
		t.Run("parse test", func(t *testing.T) {
			type set map[string]bool
			newSet := func(values ...string) set {
				s := make(set)
				for _, val := range values {
					s[val] = true
				}
				return s
			}
			testCases := []struct {
				Name    string
				Query   string
				Sources set
			}{
				{
					Name:    "simple query",
					Query:   "select * from data-engineering.testing.table1",
					Sources: newSet("data-engineering.testing.table1"),
				},
				{
					Name:    "simple query with quotes",
					Query:   "select * from `data-engineering.testing.table1`",
					Sources: newSet("data-engineering.testing.table1"),
				},
				{
					Name:    "simple query without project name",
					Query:   "select * from testing.table1",
					Sources: newSet(),
				},
				{
					Name:    "simple query with simple join",
					Query:   "select * from data-engineering.testing.table1 join data-engineering.testing.table2 on some_field",
					Sources: newSet("data-engineering.testing.table1", "data-engineering.testing.table2"),
				},
				{
					Name:    "simple query with outer join",
					Query:   "select * from data-engineering.testing.table1 outer join data-engineering.testing.table2 on some_field",
					Sources: newSet("data-engineering.testing.table1", "data-engineering.testing.table2"),
				},
				{
					Name:    "subquery",
					Query:   "select * from (select order_id from data-engineering.testing.orders)",
					Sources: newSet("data-engineering.testing.orders"),
				},
				{
					Name:    "`with` clause + simple query",
					Query:   "with `information.foo.bar` as (select * from `data-engineering.testing.data`) select * from `information.foo.bar`",
					Sources: newSet("data-engineering.testing.data"),
				},
				{
					Name:    "`with` clause with missing project name",
					Query:   "with `foo.bar` as (select * from `data-engineering.testing.data`) select * from `foo.bar`",
					Sources: newSet("data-engineering.testing.data"),
				},
				{
					Name:    "project name with dashes",
					Query:   "select * from `foo-bar.baz.data`",
					Sources: newSet("foo-bar.baz.data"),
				},
				{
					Name:    "dataset and project name with dashes",
					Query:   "select * from `foo-bar.bar-baz.data",
					Sources: newSet("foo-bar.bar-baz.data"),
				},
				{
					Name:    "`with` clause + join",
					Query:   "with dedup_source as (select * from `project.fire.fly`) select * from dedup_source join `project.maximum.overdrive` on dedup_source.left = `project.maximum.overdrive`.right",
					Sources: newSet("project.fire.fly", "project.maximum.overdrive"),
				},
				{
					Name:    "double `with` + pseudoreference",
					Query:   "with s1 as (select * from internal.pseudo.ref), with internal.pseudo.ref as (select * from `project.another.name`) select * from s1",
					Sources: newSet("project.another.name"),
				},
				{
					Name:    "simple query that ignores from upstream",
					Query:   "select * from /* @ignoreupstream */ data-engineering.testing.table1",
					Sources: newSet(),
				},
				{
					Name:    "simple query that ignores from upstream with quotes",
					Query:   "select * from /* @ignoreupstream */ `data-engineering.testing.table1`",
					Sources: newSet(),
				},
				{
					Name:    "simple query with simple join that ignores from upstream",
					Query:   "select * from /* @ignoreupstream */ data-engineering.testing.table1 join data-engineering.testing.table2 on some_field",
					Sources: newSet("data-engineering.testing.table2"),
				},
				{
					Name:    "simple query with simple join that has comments but does not ignores upstream",
					Query:   "select * from /*  */ data-engineering.testing.table1 join data-engineering.testing.table2 on some_field",
					Sources: newSet("data-engineering.testing.table1", "data-engineering.testing.table2"),
				},
				{
					Name:    "simple query with simple join that ignores upstream of join",
					Query:   "select * from data-engineering.testing.table1 join /* @ignoreupstream */ data-engineering.testing.table2 on some_field",
					Sources: newSet("data-engineering.testing.table1"),
				},
				{
					Name: "simple query with an ignoreupstream for an alias should still consider it as dependency",
					Query: `
					WITH my_temp_table AS (
						SELECT id, name FROM data-engineering.testing.an_upstream_table
					)
					SELECT id FROM /* @ignoreupstream */ my_temp_table
					`,
					Sources: newSet("data-engineering.testing.an_upstream_table"),
				},
				{
					Name: "simple query should have alias in the actual name rather than with alias",
					Query: `
					WITH my_temp_table AS (
						SELECT id, name FROM /* @ignoreupstream */ data-engineering.testing.an_upstream_table
					)
					SELECT id FROM my_temp_table
					`,
					Sources: newSet(),
				},
				{
					Name:    "simple query with simple join that ignores upstream of join",
					Query:   "WITH my_temp_table AS ( SELECT id, name FROM /* @ignoreupstream */ data-engineering.testing.an_upstream_table ) SELECT id FROM /* @ignoreupstream */ my_temp_table",
					Sources: newSet(),
				},
				{
					Name: "simple query with another query inside comment",
					Query: `
					select * from data-engineering.testing.tableABC
					-- select * from data-engineering.testing.table1 join data-engineering.testing.table2 on some_field
					`,
					Sources: newSet("data-engineering.testing.tableABC"),
				},
				{
					Name: "query with another query inside comment and a join that uses helper",
					Query: `
					select * from data-engineering.testing.tableABC
					/* select * from data-engineering.testing.table1 join data-engineering.testing.table2 on some_field */
					join /* @ignoreupstream */ data-engineering.testing.table2 on some_field
					`,
					Sources: newSet("data-engineering.testing.tableABC"),
				},
			}

			for _, test := range testCases {
				t.Run(test.Name, func(t *testing.T) {
					data := models.GenerateDependenciesRequest{
						Assets: map[string]string{
							"query.sql": test.Query,
						},
						Config: models.JobSpecConfigs{
							{
								Name:  "PROJECT",
								Value: "proj",
							},
							{
								Name:  "DATASET",
								Value: "datas",
							},
							{
								Name:  "TABLE",
								Value: "tab",
							},
						},
					}
					b2b := &bq2bq.BQ2BQ{}
					deps, err := b2b.FindDependenciesWithRegex(data)
					assert.Nil(t, err)
					assert.Equal(t, test.Sources, newSet(deps...))
				})
			}
		})
	})

	t.Run("FindDependenciesWithDryRun", func(t *testing.T) {
		type args struct {
			query string
		}
		tests := []struct {
			name       string
			args       args
			wantFromBQ []*bigquery.Table
			wantFromFn []string
			wantErr    bool
		}{
			{
				name: "deps",
				args: args{
					query: `
		select
		   t1.hakai,
		   t1.rasengan,
		   t1.` + "`over`" + `,
		   t1.load_timestamp as ` + "`event_timestamp`" + `
		from
		   ` + "`project.playground.sample_select`" + ` as t1
		
		JOIN ` + "`project.playground.sample_select_level_1`" + ` as t2
		ON t1.hakai = t2.hakai
		
		WHERE
		   DATE(load_timestamp) >= '2021-02-03'
		   AND DATE(load_timestamp) < '2021-02-05'
		`,
				},
				wantFromBQ: []*bigquery.Table{
					{
						ProjectID: "project",
						DatasetID: "playground",
						TableID:   "sample_select",
					},
					{
						ProjectID: "project",
						DatasetID: "playground",
						TableID:   "sample_select_level_1",
					},
				},
				wantFromFn: []string{
					"project:playground.sample_select",
					"project:playground.sample_select_level_1",
				},
				wantErr: false,
			},

			{
				name: "deps",
				args: args{
					query: `
SELECT * FROM ` + "`project.playground.sample_replace_view`" + ` LIMIT 1000
`,
				},
				wantFromBQ: []*bigquery.Table{
					{
						ProjectID: "project",
						DatasetID: "playground",
						TableID:   "sample_select",
					},
					{
						ProjectID: "project",
						DatasetID: "playground",
						TableID:   "sample_replace",
					},
				},
				wantFromFn: []string{
					"project:playground.sample_select",
					"project:playground.sample_replace",
				},
				wantErr: false,
			},
		}
		for _, tt := range tests {
			ctx := context.Background()
			t.Run(tt.name, func(t *testing.T) {
				job := new(bqJob)
				job.On("LastStatus").Return(&bigquery.JobStatus{
					Errors: nil,
					Statistics: &bigquery.JobStatistics{
						Details: &bigquery.QueryStatistics{
							ReferencedTables: tt.wantFromBQ,
						},
					},
				})
				defer job.AssertExpectations(t)

				qry := new(bqQuery)
				qry.On("Run", ctx).Return(job, nil)
				qry.On("SetQueryConfig", mock.AnythingOfType("bqiface.QueryConfig")).Once()
				defer qry.AssertExpectations(t)

				client := new(bqClientMock)
				client.On("Query", tt.args.query).Return(qry)
				defer client.AssertExpectations(t)

				b := &bq2bq.BQ2BQ{}
				got, err := b.FindDependenciesWithDryRun(ctx, client, tt.args.query)
				if (err != nil) != tt.wantErr {
					t.Errorf("FindDependenciesWithDryRun() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if !reflect.DeepEqual(got, tt.wantFromFn) {
					t.Errorf("FindDependenciesWithDryRun() got = %v, want %v", got, tt.wantFromFn)
				}
			})
		}
	})

	t.Run("GenerateDependencies", func(t *testing.T) {
		t.Run("should generate dependencies using BQ APIs for select statements", func(t *testing.T) {
			expectedDeps := []string{"proj:dataset.table1"}
			data := models.GenerateDependenciesRequest{
				Assets: map[string]string{
					"query.sql": "Select * from proj.dataset.table1",
				},
				Config: models.JobSpecConfigs{
					{
						Name:  "PROJECT",
						Value: "proj",
					},
					{
						Name:  "DATASET",
						Value: "datas",
					},
					{
						Name:  "TABLE",
						Value: "tab",
					},
				},
				Project: models.ProjectSpec{Secret: models.ProjectSecrets{
					{
						Name:  bq2bq.SecretName,
						Value: "some_secret",
					},
				}},
			}

			job := new(bqJob)
			job.On("LastStatus").Return(&bigquery.JobStatus{
				Errors: nil,
				Statistics: &bigquery.JobStatistics{
					Details: &bigquery.QueryStatistics{
						ReferencedTables: []*bigquery.Table{
							{
								ProjectID: "proj",
								DatasetID: "dataset",
								TableID:   "table1",
							},
						},
					},
				},
			})
			defer job.AssertExpectations(t)

			qry := new(bqQuery)
			qry.On("Run", mock.Anything).Return(job, nil)
			qry.On("SetQueryConfig", mock.AnythingOfType("bqiface.QueryConfig")).Once()
			defer qry.AssertExpectations(t)

			client := new(bqClientMock)
			client.On("Query", data.Assets[bq2bq.QueryFileName]).Return(qry)
			defer client.AssertExpectations(t)

			bqClientFac := new(bqClientFactoryMock)
			bqClientFac.On("New", mock.Anything, "some_secret").Return(client, nil)
			defer bqClientFac.AssertExpectations(t)

			b := &bq2bq.BQ2BQ{
				ClientFac: bqClientFac,
			}
			got, err := b.GenerateDependencies(data)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if !reflect.DeepEqual(got.Dependencies, expectedDeps) {
				t.Errorf("got = %v, want %v", got, expectedDeps)
			}
		})
		t.Run("should generate dependencies using BQ APIs for select statements then reuse cache for the next time", func(t *testing.T) {
			expectedDeps := []string{"proj:dataset.table1"}
			data := models.GenerateDependenciesRequest{
				Assets: map[string]string{
					"query.sql": "Select * from proj.dataset.table1",
				},
				Config: models.JobSpecConfigs{
					{
						Name:  "PROJECT",
						Value: "proj",
					},
					{
						Name:  "DATASET",
						Value: "datas",
					},
					{
						Name:  "TABLE",
						Value: "tab",
					},
				},
				Project: models.ProjectSpec{Secret: models.ProjectSecrets{
					{
						Name:  bq2bq.SecretName,
						Value: "some_secret",
					},
				}},
			}

			job := new(bqJob)
			job.On("LastStatus").Return(&bigquery.JobStatus{
				Errors: nil,
				Statistics: &bigquery.JobStatistics{
					Details: &bigquery.QueryStatistics{
						ReferencedTables: []*bigquery.Table{
							{
								ProjectID: "proj",
								DatasetID: "dataset",
								TableID:   "table1",
							},
						},
					},
				},
			})
			defer job.AssertExpectations(t)

			qry := new(bqQuery)
			qry.On("Run", mock.Anything).Return(job, nil).Once()
			qry.On("SetQueryConfig", mock.AnythingOfType("bqiface.QueryConfig")).Once()
			defer qry.AssertExpectations(t)

			client := new(bqClientMock)
			client.On("Query", data.Assets[bq2bq.QueryFileName]).Return(qry).Once()
			defer client.AssertExpectations(t)

			bqClientFac := new(bqClientFactoryMock)
			bqClientFac.On("New", mock.Anything, "some_secret").Return(client, nil).Once()
			defer bqClientFac.AssertExpectations(t)

			b := &bq2bq.BQ2BQ{
				ClientFac: bqClientFac,
				C:         cache.New(bq2bq.CacheTTL, bq2bq.CacheCleanUp),
			}
			got, err := b.GenerateDependencies(data)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if !reflect.DeepEqual(got.Dependencies, expectedDeps) {
				t.Errorf("got = %v, want %v", got, expectedDeps)
			}

			// should be cached
			got, err = b.GenerateDependencies(data)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			if !reflect.DeepEqual(got.Dependencies, expectedDeps) {
				t.Errorf("got = %v, want %v", got, expectedDeps)
			}
		})
		t.Run("should generate dependencies using regex first then BQ APIs for Scripts", func(t *testing.T) {
			expectedDeps := []string{
				"proj:dataset.table1",
				"proj:dataset.table2",
			}
			data := models.GenerateDependenciesRequest{
				Assets: map[string]string{
					"query.sql": `
DECLARE t1 timestamp;
Select * from proj.dataset.table1;
Select * from proj.dataset.table2;
`,
				},
				Config: models.JobSpecConfigs{
					{
						Name:  "PROJECT",
						Value: "proj",
					},
					{
						Name:  "DATASET",
						Value: "datas",
					},
					{
						Name:  "TABLE",
						Value: "tab",
					},
				},
				Project: models.ProjectSpec{Secret: models.ProjectSecrets{
					{
						Name:  bq2bq.SecretName,
						Value: "some_secret",
					},
				}},
			}

			// no tables when used with scripts
			jobWithNoReference := new(bqJob)
			jobWithNoReference.On("LastStatus").Return(&bigquery.JobStatus{
				Errors: nil,
				Statistics: &bigquery.JobStatistics{
					Details: &bigquery.QueryStatistics{
						ReferencedTables: nil,
					},
				},
			})
			defer jobWithNoReference.AssertExpectations(t)

			// used with fake select stmts
			jobWithTable1 := new(bqJob)
			jobWithTable1.On("LastStatus").Return(&bigquery.JobStatus{
				Errors: nil,
				Statistics: &bigquery.JobStatistics{
					Details: &bigquery.QueryStatistics{
						ReferencedTables: []*bigquery.Table{
							{
								ProjectID: "proj",
								DatasetID: "dataset",
								TableID:   "table1",
							},
						},
					},
				},
			})
			defer jobWithTable1.AssertExpectations(t)
			jobWithTable2 := new(bqJob)
			jobWithTable2.On("LastStatus").Return(&bigquery.JobStatus{
				Errors: nil,
				Statistics: &bigquery.JobStatistics{
					Details: &bigquery.QueryStatistics{
						ReferencedTables: []*bigquery.Table{
							{
								ProjectID: "proj",
								DatasetID: "dataset",
								TableID:   "table2",
							},
						},
					},
				},
			})
			defer jobWithTable2.AssertExpectations(t)

			// used for the first time and return no tables
			qryScript := new(bqQuery)
			qryScript.On("Run", mock.Anything).Return(jobWithNoReference, nil)
			qryScript.On("SetQueryConfig", mock.AnythingOfType("bqiface.QueryConfig")).Once()
			defer qryScript.AssertExpectations(t)

			// used with fake select stmts
			qrySelect1 := new(bqQuery)
			qrySelect1.On("Run", mock.Anything).Return(jobWithTable1, nil)
			qrySelect1.On("SetQueryConfig", mock.AnythingOfType("bqiface.QueryConfig")).Once()
			defer qrySelect1.AssertExpectations(t)
			qrySelect2 := new(bqQuery)
			qrySelect2.On("Run", mock.Anything).Return(jobWithTable2, nil)
			qrySelect2.On("SetQueryConfig", mock.AnythingOfType("bqiface.QueryConfig")).Once()
			defer qrySelect2.AssertExpectations(t)

			client := new(bqClientMock)
			client.On("Query", data.Assets[bq2bq.QueryFileName]).Return(qryScript)
			client.On("Query", fmt.Sprintf(bq2bq.FakeSelectStmt, "proj.dataset.table1")).Return(qrySelect1)
			client.On("Query", fmt.Sprintf(bq2bq.FakeSelectStmt, "proj.dataset.table2")).Return(qrySelect2)
			defer client.AssertExpectations(t)

			bqClientFac := new(bqClientFactoryMock)
			bqClientFac.On("New", mock.Anything, "some_secret").Return(client, nil)
			defer bqClientFac.AssertExpectations(t)

			b := &bq2bq.BQ2BQ{
				ClientFac: bqClientFac,
			}
			got, err := b.GenerateDependencies(data)
			if err != nil {
				t.Errorf("error = %v", err)
				return
			}
			sort.Strings(got.Dependencies)
			if !reflect.DeepEqual(got.Dependencies, expectedDeps) {
				t.Errorf("got = %v, want %v", got, expectedDeps)
			}
		})
	})
}
