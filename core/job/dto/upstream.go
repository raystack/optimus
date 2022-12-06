package dto

// TODO: remove RawUpstream and use Upstream (Unresolved)
type RawUpstream struct {
	ProjectName string
	JobName     string
	ResourceURN string
}

func (r RawUpstream) IsStatic() bool {
	return r.JobName != ""
}

type Downstream struct {
	Name string

	ProjectName   string
	NamespaceName string

	TaskName string
}
