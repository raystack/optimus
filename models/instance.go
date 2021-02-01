package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

const (
	// run data types
	InstanceDataTypeEnv  = "env"
	InstanceDataTypeFile = "file"

	// InstanceDataTypeEnvFileName is run data env type file name
	InstanceDataTypeEnvFileName = ".env"

	// iso 2021-01-14T02:00:00+00:00
	InstanceScheduledAtTimeLayout = time.RFC3339

	InstanceStateRunning = "running"
	InstanceStateFailed  = "failed"
	InstanceStateSuccess = "success"

	// InstanceType is the kind of execution happening at the time
	InstanceTypeTransformation = "transformation"
	InstanceTypeHook           = "hook"
)

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
	Task  string
	Type  string
}

func (j *InstanceSpec) DataToJSON() (string, error) {
	blob, err := json.Marshal(j.Data)
	if err != nil {
		return "", err
	}
	return string(blob), nil
}

type InstanceService interface {
	Register(JobSpec, time.Time) (InstanceSpec, error)
	Clear(JobSpec, time.Time) error
}
