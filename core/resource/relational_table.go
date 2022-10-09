package resource

import "github.com/odpf/optimus/internal/errors"

const (
	EntityTable = "resource_table"
)

type Table struct {
	Name    Name
	Dataset Dataset

	Description string     `mapstructure:"description,omitempty"`
	Schema      Schema     `mapstructure:"schema,omitempty"`
	Cluster     *Cluster   `mapstructure:"cluster,omitempty"`
	Partition   *Partition `mapstructure:"partition,omitempty"`

	ExtraConfig map[string]interface{} `mapstructure:",remain"`
}

func (t *Table) FullName() string {
	return t.Dataset.FullName() + "." + t.Name.String()
}

func (t *Table) URN() string {
	return t.Dataset.URN() + "." + t.Name.String()
}

func (t *Table) Validate() error {
	if len(t.Schema) == 0 {
		return errors.InvalidArgument(EntityTable, "invalid schema for table "+t.FullName())
	}

	if err := t.Schema.Validate(); err != nil {
		return err
	}

	if t.Partition != nil {
		if err := t.Partition.Validate(); err != nil {
			return err
		}
	}

	if t.Cluster != nil {
		if err := t.Cluster.Validate(); err != nil {
			return err
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
		return errors.InvalidArgument(EntityTable, "name of field for partition is empty")
	}

	if p.Type == "" {
		return errors.InvalidArgument(EntityTable, "partition type is empty")
	}

	if p.Type == "range" {
		if p.Range == nil {
			return errors.InvalidArgument(EntityTable, "partition type range have no range config")
		}
	}

	return nil
}

type Range struct {
	Start    int64 `mapstructure:"start,omitempty"`
	End      int64 `mapstructure:"end,omitempty"`
	Interval int64 `mapstructure:"interval,omitempty"`
}
