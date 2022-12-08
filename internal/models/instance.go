package models

import (
	"time"
)

const (
	// run data types
	// env can be used to templatize assets and configs of task and hooks
	// at run time
	InstanceDataTypeEnv = "env"
	// files will be used to store temporary data passed around for inter-task
	// communication
	InstanceDataTypeFile = "file"

	// InstanceDataTypeEnvFileName is run data env type file name
	InstanceDataTypeEnvFileName    = ".env"
	InstanceDataTypeSecretFileName = ".secret"

	// iso 2021-01-14T02:00:00+00:00
	InstanceScheduledAtTimeLayout = time.RFC3339

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

// TemplateEngine compiles raw text templates using provided values
type TemplateEngine interface {
	CompileFiles(files map[string]string, context map[string]interface{}) (map[string]string, error)
	CompileString(input string, context map[string]interface{}) (string, error)
}
