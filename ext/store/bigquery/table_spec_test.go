package bigquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/ext/store/bigquery"
)

func TestRelationalTable(t *testing.T) {
	t.Run("when invalid", func(t *testing.T) {
		t.Run("returns validation error for empty schema", func(t *testing.T) {
			table := bigquery.Table{
				Name:        "t-optimus.playground.characters",
				Schema:      nil,
				Cluster:     &bigquery.Cluster{Using: []string{"tags"}},
				Partition:   &bigquery.Partition{Field: "time", Type: "DAY"},
				ExtraConfig: nil,
			}
			err := table.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "empty schema for table t-optimus.playground.characters")
		})
		t.Run("returns validation error for invalid schema", func(t *testing.T) {
			table := bigquery.Table{
				Name:        "t-optimus.playground.characters",
				Schema:      bigquery.Schema{{Name: "", Type: "string", Mode: "nullable"}},
				Cluster:     &bigquery.Cluster{Using: []string{"tags"}},
				Partition:   &bigquery.Partition{Field: "time", Type: "DAY"},
				ExtraConfig: nil,
			}
			err := table.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid schema for table t-optimus.playground.characters")
		})
		t.Run("returns validation error for invalid cluster", func(t *testing.T) {
			table := bigquery.Table{
				Name:        "t-optimus.playground.characters",
				Schema:      bigquery.Schema{{Name: "id", Type: "string", Mode: "nullable"}},
				Cluster:     &bigquery.Cluster{Using: []string{}},
				Partition:   &bigquery.Partition{Field: "time", Type: "DAY"},
				ExtraConfig: nil,
			}
			err := table.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "invalid cluster for table t-optimus.playground.characters")
		})
	})
	t.Run("returns no validation error when correct", func(t *testing.T) {
		table := bigquery.Table{
			Name:        "t-optimus.playground.characters",
			Schema:      bigquery.Schema{{Name: "id", Type: "string", Mode: "nullable"}},
			Cluster:     &bigquery.Cluster{Using: []string{"tags"}},
			Partition:   &bigquery.Partition{Field: "time", Type: "DAY"},
			ExtraConfig: nil,
		}
		err := table.Validate()
		assert.Nil(t, err)

		assert.Equal(t, "t-optimus.playground.characters", table.FullName())
	})
	t.Run("passes validation for empty field name in partition", func(t *testing.T) {
		table := bigquery.Table{
			Name:        "t-optimus.playground.characters",
			Schema:      bigquery.Schema{{Name: "id", Type: "string", Mode: "nullable"}},
			Cluster:     &bigquery.Cluster{Using: []string{"tags"}},
			Partition:   &bigquery.Partition{Field: "", Type: "DAY"},
			ExtraConfig: nil,
		}
		err := table.Validate()
		assert.Nil(t, err)
	})
}

func TestTableClustering(t *testing.T) {
	t.Run("returns error when invalid", func(t *testing.T) {
		cluster := bigquery.Cluster{Using: []string{}}

		err := cluster.Validate()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "cluster config is empty")
	})
	t.Run("returns error when invalid value for cluster column", func(t *testing.T) {
		cluster := bigquery.Cluster{Using: []string{""}}

		err := cluster.Validate()
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "cluster config has invalid value")
	})
	t.Run("no validation error when valid", func(t *testing.T) {
		cluster := bigquery.Cluster{Using: []string{"time"}}
		assert.Nil(t, cluster.Validate())
	})
}

func TestTablePartitioning(t *testing.T) {
	t.Run("when invalid", func(t *testing.T) {
		t.Run("returns error for invalid range", func(t *testing.T) {
			p := bigquery.Partition{
				Field: "TIME",
				Type:  "RANGE",
				Range: nil,
			}
			err := p.Validate()
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "partition type range have no range config for TIME")
		})
	})
	t.Run("return no error when valid", func(t *testing.T) {
		p := bigquery.Partition{
			Field: "TIME",
			Type:  "range",
			Range: &bigquery.Range{Start: 0, End: 5},
		}
		err := p.Validate()
		assert.Nil(t, err)
	})
	t.Run("returns no error for empty field", func(t *testing.T) {
		p := bigquery.Partition{
			Field: "",
			Type:  "DAY",
		}
		err := p.Validate()
		assert.Nil(t, err)
	})
	t.Run("returns no error for empty type", func(t *testing.T) {
		p := bigquery.Partition{
			Field: "TIME",
			Type:  "",
		}
		err := p.Validate()
		assert.Nil(t, err)
	})
}
