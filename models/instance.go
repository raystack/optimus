package models

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

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
	InstanceDataTypeEnvFileName    = ".env"
	InstanceDataTypeSecretFileName = ".secret"

	// iso 2021-01-14T02:00:00+00:00
	InstanceScheduledAtTimeLayout = time.RFC3339

	// InstanceType is the kind of execution happening at the time
	InstanceTypeTask InstanceType = "task"
	InstanceTypeHook InstanceType = "hook"

	// job run created by a batch schedule
	TriggerSchedule JobRunTrigger = "schedule"
	// job run created by a manual user request
	TriggerManual JobRunTrigger = "manual"
)

type JobRunTrigger string

func (j JobRunTrigger) String() string {
	return string(j)
}

const (

	// assignment , non terminating state
	RunStatePending JobRunState = "pending"

	// non assignment, non terminating states
	RunStateAccepted JobRunState = "accepted"
	RunStateRunning  JobRunState = "running"

	// terminate states
	RunStateSuccess JobRunState = "success"
	RunStateFailed  JobRunState = "failed"
)

type JobRunState string

func (j JobRunState) String() string {
	return string(j)
}

// JobRun is a representation of job in execution state, this is created
// when a run is requested and shared for all tasks/hooks in a job
type JobRun struct {
	ID          uuid.UUID
	Spec        JobSpec
	Trigger     JobRunTrigger
	Status      JobRunState
	Instances   []InstanceSpec
	ScheduledAt time.Time
	ExecutedAt  time.Time
}

func (j *JobRun) GetInstance(instanceName string, instanceType InstanceType) (InstanceSpec, error) {
	for _, instance := range j.Instances {
		if instance.Name == instanceName && instance.Type == instanceType {
			return instance, nil
		}
	}
	return InstanceSpec{}, errors.New("instance not found")
}

func (j JobRun) String() string {
	return fmt.Sprintf("id_%s:trigger_%s:scheduled_at_%s", j.ID, j.Trigger, j.ScheduledAt)
}

type InstanceType string

func (i InstanceType) String() string {
	return string(i)
}

func ToInstanceType(val string) (InstanceType, error) {
	switch strings.ToLower(val) {
	case "task":
		return InstanceTypeTask, nil
	case "hook":
		return InstanceTypeHook, nil
	}
	return "", fmt.Errorf("failed to convert to instance type, invalid val: %s", val)
}

// InstanceSpec is a representation of task/hook in execution state
type InstanceSpec struct {
	ID   uuid.UUID
	Name string
	Type InstanceType

	Status JobRunState
	Data   []InstanceSpecData

	ExecutedAt time.Time
	UpdatedAt  time.Time
}

type InstanceSpecData struct {
	Name  string
	Value string
	Type  string
}

func (j *InstanceSpec) DataToJSON() ([]byte, error) {
	if len(j.Data) == 0 {
		return nil, nil
	}
	return json.Marshal(j.Data)
}

type JobRunInput struct {
	ConfigMap  map[string]string
	FileMap    map[string]string
	SecretsMap map[string]string
}

type RunService interface {
	// GetScheduledRun find if already present or create a new scheduled run
	GetScheduledRun(ctx context.Context, namespace NamespaceSpec, JobID JobSpec, scheduledAt time.Time) (JobRun, error)

	// GetByID returns job run, normally gets requested for manual runs
	GetByID(ctx context.Context, JobRunID uuid.UUID) (JobRun, NamespaceSpec, error)

	// Register creates a new instance in provided job run
	Register(ctx context.Context, namespace NamespaceSpec, jobRun JobRun, instanceType InstanceType, instanceName string) (InstanceSpec, error)

	// Compile prepares instance execution context environment
	Compile(ctx context.Context, namespaceSpec NamespaceSpec, jobRun JobRun, instanceSpec InstanceSpec) (jobRunInput *JobRunInput, err error)
}

// TemplateEngine compiles raw text templates using provided values
type TemplateEngine interface {
	CompileFiles(files map[string]string, context map[string]interface{}) (map[string]string, error)
	CompileString(input string, context map[string]interface{}) (string, error)
}
