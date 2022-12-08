package models

import (
	"errors"
)

var (
	ErrNoSuchSpec  = errors.New("spec not found")
	ErrNoSuchJob   = errors.New("job not found")
	ErrNoJobs      = errors.New("no job found")
	ErrNoResources = errors.New("no resources found")
	ErrNoSuchAsset = errors.New("asset not found")
	ErrNoSuchHook  = errors.New("hook not found")
)

const (
	JobDatetimeLayout = "2006-01-02"

	JobSpecDefaultVersion = 1
)

type JobSpecConfigs []JobSpecConfigItem

func (j JobSpecConfigs) Get(name string) (string, bool) {
	for _, conf := range j {
		if conf.Name == name {
			return conf.Value, true
		}
	}
	return "", false
}

type JobSpecConfigItem struct {
	Name  string
	Value string
}

type JobSpecAsset struct {
	Name  string
	Value string
}

type JobAssets struct {
	data []JobSpecAsset
}

func (a *JobAssets) ToMap() map[string]string {
	if len(a.data) == 0 {
		return nil
	}
	mp := map[string]string{}
	for _, asset := range a.data {
		mp[asset.Name] = asset.Value
	}
	return mp
}

func (a *JobAssets) GetAll() []JobSpecAsset {
	return a.data
}

func (JobAssets) New(data []JobSpecAsset) *JobAssets {
	return &JobAssets{
		data: data,
	}
}

type JobDeploymentStatus string

func (j JobDeploymentStatus) String() string {
	return string(j)
}

const (
	JobDeploymentStatusCancelled  JobDeploymentStatus = "Cancelled"
	JobDeploymentStatusInQueue    JobDeploymentStatus = "In Queue"
	JobDeploymentStatusInProgress JobDeploymentStatus = "In Progress"
	JobDeploymentStatusSucceed    JobDeploymentStatus = "Succeed"
	JobDeploymentStatusFailed     JobDeploymentStatus = "Failed"
)
