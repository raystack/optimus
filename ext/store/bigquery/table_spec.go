package bigquery

import (
	"strings"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/internal/errors"
)

const (
	EntityTable = "resource_table"
)

type Table struct {
	Name resource.Name

	Description string     `mapstructure:"description,omitempty"`
	Schema      Schema     `mapstructure:"schema,omitempty"`
	Cluster     *Cluster   `mapstructure:"cluster,omitempty"`
	Partition   *Partition `mapstructure:"partition,omitempty"`

	ExtraConfig map[string]interface{} `mapstructure:",remain"`
}

func (t *Table) FullName() string {
	return t.Name.String()
}

func (t *Table) Validate() error {
	if len(t.Schema) == 0 {
		return errors.InvalidArgument(EntityTable, "empty schema for table "+t.FullName())
	}

	if err := t.Schema.Validate(); err != nil {
		return errors.AddErrContext(err, EntityTable, "invalid schema for table "+t.FullName())
	}

	if t.Partition != nil {
		if err := t.Partition.Validate(); err != nil {
			return errors.AddErrContext(err, EntityTable, "invalid partition for table "+t.FullName())
		}
	}

	if t.Cluster != nil {
		if err := t.Cluster.Validate(); err != nil {
			return errors.AddErrContext(err, EntityTable, "invalid cluster for table "+t.FullName())
		}
	}

	return nil
}

type Cluster struct {
	Using []string `mapstructure:"using,omitempty"`
}

func (c Cluster) Validate() error {
	if len(c.Using) == 0 {
		return errors.InvalidArgument(EntityTable, "cluster config is empty")
	}
	for _, clause := range c.Using {
		if clause == "" {
			return errors.InvalidArgument(EntityTable, "cluster config has invalid value")
		}
	}

	return nil
}

type Partition struct {
	Field string `mapstructure:"field,omitempty"`

	Type       string `mapstructure:"type,omitempty"`
	Expiration int64  `mapstructure:"expiration,omitempty"`

	Range *Range `mapstructure:"range,omitempty"`
}

func (p Partition) Validate() error {
	if p.Field == "" {
		return errors.InvalidArgument(EntityTable, "partition field name is empty")
	}

	if p.Type == "" {
		return errors.InvalidArgument(EntityTable, "partition type is empty for "+p.Field)
	}

	if strings.EqualFold(p.Type, "range") {
		if p.Range == nil {
			return errors.InvalidArgument(EntityTable, "partition type range have no range config for "+p.Field)
		}
	}

	return nil
}

type Range struct {
	Start    int64 `mapstructure:"start,omitempty"`
	End      int64 `mapstructure:"end,omitempty"`
	Interval int64 `mapstructure:"interval,omitempty"`
}
