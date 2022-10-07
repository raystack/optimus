package resource

type Table struct {
	Name    Name
	Dataset Dataset

	Description string
	Schema      Schema
	Cluster     *Cluster
	Partition   *Partition
}

type Cluster struct {
	Using []string
}

type Partition struct {
	Field string

	Type       string
	Expiration int64

	Range *Range
}

type Range struct {
	Start    int64
	End      int64
	Interval int64
}
