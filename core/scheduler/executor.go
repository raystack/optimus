package scheduler

import (
	"strings"
	"time"

	"github.com/raystack/optimus/internal/errors"
	"github.com/raystack/optimus/internal/utils"
)

const (
	ExecutorTask ExecutorType = "task"
	ExecutorHook ExecutorType = "hook"
)

type ExecutorType string

func (e ExecutorType) String() string {
	return string(e)
}

func ExecutorTypeFrom(val string) (ExecutorType, error) {
	switch strings.ToLower(val) {
	case string(ExecutorTask):
		return ExecutorTask, nil
	case string(ExecutorHook):
		return ExecutorHook, nil
	}
	return "", errors.InvalidArgument(EntityJobRun, "failed to convert to executor type, invalid value: "+val)
}

type Executor struct {
	Name string
	Type ExecutorType
}

func ExecutorFrom(name string, executorType ExecutorType) (Executor, error) {
	if name == "" {
		return Executor{}, errors.InvalidArgument(EntityJobRun, "executor name is invalid")
	}

	return Executor{
		Name: name,
		Type: executorType,
	}, nil
}

func ExecutorFromEnum(name, enum string) (Executor, error) {
	if enum == "" {
		return Executor{}, errors.InvalidArgument(EntityJobRun, "executor type is empty")
	}

	_typ, err := ExecutorTypeFrom(utils.FromEnumProto(enum, "TYPE"))
	if err != nil {
		return Executor{}, err
	}

	return ExecutorFrom(name, _typ)
}

type RunConfig struct {
	Executor Executor

	ScheduledAt time.Time
	JobRunID    JobRunID
}

func RunConfigFrom(executor Executor, scheduledAt time.Time, runID string) (RunConfig, error) {
	jobRunID, err := JobRunIDFromString(runID) // runID can be empty or a valid uuid
	if err != nil {
		return RunConfig{}, errors.InvalidArgument(EntityJobRun, "invalid job run ID "+runID)
	}

	return RunConfig{
		Executor:    executor,
		ScheduledAt: scheduledAt,
		JobRunID:    jobRunID,
	}, nil
}

type ConfigMap map[string]string

type ExecutorInput struct {
	Configs ConfigMap
	Secrets ConfigMap
	Files   ConfigMap
}
