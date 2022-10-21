package dto

type UnresolvedDependency struct {
	ProjectName string
	JobName     string
	ResourceURN string
}

func (u UnresolvedDependency) IsStaticDependency() bool {
	if u.JobName != "" {
		return true
	}
	return false
}
