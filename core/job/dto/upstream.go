package dto

type RawUpstream struct {
	ProjectName string
	JobName     string
	ResourceURN string
}

func (r RawUpstream) IsStaticDependency() bool {
	return r.JobName != ""
}
