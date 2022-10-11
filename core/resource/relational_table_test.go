package resource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/resource"
)

func TestRelationalTable(t *testing.T) {
	t.Run("when invalid", func(t *testing.T) {
		t.Run("returns validation error for empty schema", func(t *testing.T) {
			ds, err := resource.DataSetFrom(resource.BigQuery, "t-optimus", "playground")
			assert.Nil(t, err)

			table := resource.Table{
				Name:        "characters",
				Dataset:     ds,
				Schema:      nil,
				Cluster:     &resource.Cluster{Using: []string{"tags"}},
				Partition:   &resource.Partition{Field: "time", Type: "DAY"},
				ExtraConfig: nil,
			}
			err = table.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_table: empty schema for "+
				"table t-optimus.playground.characters")
		})
		t.Run("returns validation error for invalid schema", func(t *testing.T) {
			ds, err := resource.DataSetFrom(resource.BigQuery, "t-optimus", "playground")
			assert.Nil(t, err)

			table := resource.Table{
				Name:        "characters",
				Dataset:     ds,
				Schema:      resource.Schema{{Name: "", Type: "string", Mode: "nullable"}},
				Cluster:     &resource.Cluster{Using: []string{"tags"}},
				Partition:   &resource.Partition{Field: "time", Type: "DAY"},
				ExtraConfig: nil,
			}
			err = table.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_table: "+
				"invalid schema for table t-optimus.playground.characters")
		})
		t.Run("returns validation error for invalid partition", func(t *testing.T) {
			ds, err := resource.DataSetFrom(resource.BigQuery, "t-optimus", "playground")
			assert.Nil(t, err)

			table := resource.Table{
				Name:        "characters",
				Dataset:     ds,
				Schema:      resource.Schema{{Name: "id", Type: "string", Mode: "nullable"}},
				Cluster:     &resource.Cluster{Using: []string{"tags"}},
				Partition:   &resource.Partition{Field: "", Type: "DAY"},
				ExtraConfig: nil,
			}
			err = table.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_table: invalid partition for "+
				"table t-optimus.playground.characters")
		})
		t.Run("returns validation error for invalid cluster", func(t *testing.T) {
			ds, err := resource.DataSetFrom(resource.BigQuery, "t-optimus", "playground")
			assert.Nil(t, err)

			table := resource.Table{
				Name:        "characters",
				Dataset:     ds,
				Schema:      resource.Schema{{Name: "id", Type: "string", Mode: "nullable"}},
				Cluster:     &resource.Cluster{Using: []string{}},
				Partition:   &resource.Partition{Field: "time", Type: "DAY"},
				ExtraConfig: nil,
			}
			err = table.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_table: invalid cluster for "+
				"table t-optimus.playground.characters")
		})
	})
	t.Run("returns no validation error when correct", func(t *testing.T) {
		ds, err := resource.DataSetFrom(resource.BigQuery, "t-optimus", "playground")
		assert.Nil(t, err)

		table := resource.Table{
			Name:        "characters",
			Dataset:     ds,
			Schema:      resource.Schema{{Name: "id", Type: "string", Mode: "nullable"}},
			Cluster:     &resource.Cluster{Using: []string{"tags"}},
			Partition:   &resource.Partition{Field: "time", Type: "DAY"},
			ExtraConfig: nil,
		}
		err = table.Validate()
		assert.Nil(t, err)

		assert.Equal(t, "t-optimus.playground.characters", table.FullName())
		assert.Equal(t, "bigquery://t-optimus:playground.characters", table.URN())
	})
}

func TestTableClustering(t *testing.T) {
	t.Run("returns error when invalid", func(t *testing.T) {
		cluster := resource.Cluster{Using: []string{}}

		err := cluster.Validate()
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource_table: cluster config is empty")
	})
	t.Run("returns error when invalid value for cluster column", func(t *testing.T) {
		cluster := resource.Cluster{Using: []string{""}}

		err := cluster.Validate()
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource_table: cluster config has invalid value")
	})
	t.Run("no validation error when valid", func(t *testing.T) {
		cluster := resource.Cluster{Using: []string{"time"}}
		assert.Nil(t, cluster.Validate())
	})
}

func TestTablePartitioning(t *testing.T) {
	t.Run("when invalid", func(t *testing.T) {
		t.Run("returns error for invalid field", func(t *testing.T) {
			p := resource.Partition{
				Field: "",
				Type:  "DAY",
			}
			err := p.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_table: partition field name is empty")
		})
		t.Run("returns error for invalid type", func(t *testing.T) {
			p := resource.Partition{
				Field: "TIME",
				Type:  "",
			}
			err := p.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_table: "+
				"partition type is empty for TIME")
		})
		t.Run("returns error for invalid range", func(t *testing.T) {
			p := resource.Partition{
				Field: "TIME",
				Type:  "RANGE",
				Range: nil,
			}
			err := p.Validate()
			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity resource_table: partition type range "+
				"have no range config for TIME")
		})
	})
	t.Run("return no error when valid", func(t *testing.T) {
		p := resource.Partition{
			Field: "TIME",
			Type:  "range",
			Range: &resource.Range{Start: 0, End: 5},
		}
		err := p.Validate()
		assert.Nil(t, err)
	})
}
