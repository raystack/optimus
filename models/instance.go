package models

import (
	"encoding/json"
	"time"

	"github.com/pkg/errors"

	"github.com/google/uuid"
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
	InstanceDataTypeEnvFileName = ".env"

	// iso 2021-01-14T02:00:00+00:00
	InstanceScheduledAtTimeLayout = time.RFC3339

	InstanceStateRunning = "running"
	InstanceStateFailed  = "failed"
	InstanceStateSuccess = "success"

	// InstanceType is the kind of execution happening at the time
	InstanceTypeTransformation InstanceType = "transformation"
	InstanceTypeHook           InstanceType = "hook"
)

type InstanceType string

func (I InstanceType) New(val string) (InstanceType, error) {
	switch val {
	case "transformation":
		fallthrough
	case "hook":
		return InstanceType(val), nil
	}
	return InstanceType(""), errors.Errorf("failed to convert to instance type, invalid val: %s", val)
}

type InstanceSpec struct {
	ID          uuid.UUID
	Job         JobSpec
	ScheduledAt time.Time
	State       string
	Data        []InstanceSpecData
}

type InstanceSpecData struct {
	Name  string
	Value string
	Type  string
}

func (j *InstanceSpec) DataToJSON() ([]byte, error) {
	return json.Marshal(j.Data)
}

type InstanceService interface {
	Register(jobSpec JobSpec, scheduledAt time.Time, taskType InstanceType) (InstanceSpec, error)
}

// TemplateEngine compiles raw text templates using provided values
type TemplateEngine interface {
	CompileFiles(files map[string]string, context map[string]interface{}) (map[string]string, error)
	CompileString(input string, context map[string]interface{}) (string, error)
}
