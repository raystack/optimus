package models

import (
	"github.com/odpf/optimus/core/progress"
)

type MetadataService interface {
	Publish(ProjectSpec, []JobSpec, progress.Observer) error
}

type JobMetadataAdapter interface {
	FromJobSpec(ProjectSpec, JobSpec) (*JobMetadata, error)
	CompileMessage(*JobMetadata) ([]byte, error)
	CompileKey(string) ([]byte, error)
}

type MetadataWriter interface {
	Write(key []byte, message []byte) error
	Flush() error
}

type JobMetadata struct {
	Urn          string
	Name         string
	Tenant       string
	Version      int
	Description  string
	Labels       []JobMetadataLabelItem
	Owner        string
	Task         JobTaskMetadata
	Schedule     JobSpecSchedule
	Behavior     JobSpecBehavior
	Dependencies []JobDependencyMetadata
	Hooks        []JobHookMetadata
}

type JobMetadataLabelItem struct {
	Name  string
	Value string
}

type JobTaskMetadata struct {
	Name        string
	Image       string
	Description string
	Destination string
	Config      JobSpecConfigs
	Window      JobSpecTaskWindow
	Priority    int
}

type JobHookMetadata struct {
	Name        string
	Image       string
	Description string
	Config      JobSpecConfigs
	Type        HookType
	DependsOn   []string
}

type JobDependencyMetadata struct {
	Tenant string
	Job    string
	Type   string
}
