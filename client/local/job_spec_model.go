package local

import "gopkg.in/yaml.v2"

type JobSpec struct {
	Version      int               `yaml:"version"`
	Name         string            `yaml:"name"`
	Owner        string            `yaml:"owner"`
	Description  string            `yaml:"description"`
	Schedule     JobSchedule       `yaml:"schedule"`
	Behavior     JobBehavior       `yaml:"behavior"`
	Task         JobTask           `yaml:"task"`
	Asset        map[string]string `yaml:"asset"`
	Labels       map[string]string `yaml:"labels"`
	Dependencies []JobDependency   `yaml:"dependencies"`
	Hooks        []JobHook         `yaml:"hooks"`
	Metadata     JobSpecMetadata   `yaml:"metadata"`
}

type JobSchedule struct {
	StartDate string `yaml:"start_date"`
	EndDate   string `yaml:"end_date"`
	Interval  string `yaml:"interval"`
}

type JobBehavior struct {
	DependsOnPast bool             `yaml:"depends_on_past"`
	Catchup       bool             `yaml:"catch_up"`
	Retry         JobBehaviorRetry `yaml:"retry"`
	Notify        []JobNotifier    `yaml:"notify"`
}

type JobBehaviorRetry struct {
	Count              int    `yaml:"count"`
	Delay              string `yaml:"delay"`
	ExponentialBackoff bool   `yaml:"exponential_backoff"`
}

type JobNotifier struct {
	On       string            `yaml:"on"`
	Config   map[string]string `yaml:"config"`
	Channels []string          `yaml:"channels"`
}

type JobTask struct {
	Name   string        `yaml:"name"`
	Config yaml.MapSlice `yaml:"config"`
	Window JobTaskWindow `yaml:"window"`
}

type JobTaskWindow struct {
	Size       string `yaml:"size"`
	Offset     string `yaml:"offset"`
	TruncateTo string `yaml:"truncate_to"`
}

type JobHook struct {
	Name   string        `yaml:"name"`
	Config yaml.MapSlice `yaml:"config"`
}

type JobDependency struct {
	JobName string         `yaml:"job"`
	Type    string         `yaml:"type"`
	HTTPDep HTTPDependency `yaml:"http"`
}

type HTTPDependency struct {
	Name          string            `yaml:"name"`
	RequestParams map[string]string `yaml:"params"`
	URL           string            `yaml:"url"`
	Headers       map[string]string `yaml:"headers"`
}

type JobSpecMetadata struct {
	Resource JobSpecResource `yaml:"resource"`
	Airflow  JobSpecAirflow  `yaml:"airflow"`
}

type JobSpecResource struct {
	Request JobSpecResourceConfig `yaml:"request"`
	Limit   JobSpecResourceConfig `yaml:"limit"`
}

type JobSpecResourceConfig struct {
	Memory string `yaml:"memory"`
	CPU    string `yaml:"cpu"`
}

type JobSpecAirflow struct {
	Pool  string `yaml:"pool"`
	Queue string `yaml:"queue"`
}
