package dto

type RawDependency struct {
	ProjectName string
	JobName     string
	ResourceURN string
}

func (u RawDependency) IsStaticDependency() bool {
	return u.JobName != ""
}
