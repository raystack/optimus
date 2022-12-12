package models

const (
	// InstanceType is the kind of execution happening at the time
	InstanceTypeTask InstanceType = "task"
	InstanceTypeHook InstanceType = "hook"
)

type JobRunState string

func (j JobRunState) String() string {
	return string(j)
}

type InstanceType string

func (i InstanceType) String() string {
	return string(i)
}

type JobRunSpecData struct {
	Name  string
	Value string
	Type  string
}
