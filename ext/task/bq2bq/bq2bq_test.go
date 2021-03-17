package bq2bq_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/ext/task/bq2bq"
	"github.com/odpf/optimus/models"
)

func TestBQ2BQ(t *testing.T) {
	t.Run("GenerateDestination", func(t *testing.T) {
		t.Run("should properly generate a destination provided correct config inputs", func(t *testing.T) {
			b2b := &bq2bq.BQ2BQ{}
			dst, err := b2b.GenerateDestination(models.UnitData{
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
			assert.Equal(t, "proj.datas.tab", dst)
		})
		t.Run("should throw an error if any on of the config is missing to generate destination", func(t *testing.T) {
			b2b := &bq2bq.BQ2BQ{}
			_, err := b2b.GenerateDestination(models.UnitData{
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
	t.Run("GenerateDependencies", func(t *testing.T) {
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
					data := models.UnitData{
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
					deps, err := b2b.GenerateDependencies(data)
					assert.Nil(t, err)
					assert.Equal(t, test.Sources, newSet(deps...))
				})
			}
		})
	})
}
