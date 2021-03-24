package models

import (
	"github.com/odpf/optimus/core/progress"
)

type MetadataService interface {
	Publish([]JobSpec, progress.Observer) error
}

type MetadataBuilder interface {
	FromJobSpec(jobSpec JobSpec) (*ResourceMetadata, error)
	CompileMessage(*ResourceMetadata) ([]byte, error)
	CompileKey(string) ([]byte, error)
}

type MetadataWriter interface {
	Write(key []byte, message []byte) error
	Flush() error
}

type ResourceMetadata struct {
	Urn          string
	Version      int
	Description  string
	Labels       []JobSpecLabelItem
	Owner        string
	Task         TaskMetadata
	Schedule     JobSpecSchedule
	Behavior     JobSpecBehavior
	Dependencies []JobDependencyMetadata
	Hooks        []HookMetadata
}

type TaskMetadata struct {
	Name        string
	Image       string
	Description string
	Destination string
	Config      JobSpecConfigs
	Window      JobSpecTaskWindow
	Priority    int
}

type HookMetadata struct {
	Name        string
	Image       string
	Description string
	Config      JobSpecConfigs
	Type        HookType
	DependsOn   []string
}

type JobDependencyMetadata struct {
	Project string
	Job     string
	Type    string
}
