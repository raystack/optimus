package dto

type RawUpstream struct {
	ProjectName string
	JobName     string
	ResourceURN string
}

func (r RawUpstream) IsStatic() bool {
	return r.JobName != ""
}
