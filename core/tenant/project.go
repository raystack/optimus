package tenant

import (
	"errors"
)

type ProjectName string

func ProjectNameFrom(name string) (ProjectName, error) {
	if name == "" {
		return "", errors.New("project name is empty")
	}
	return ProjectName(name), nil
}

func (pn ProjectName) String() string {
	return string(pn)
}

type Project struct {
	name ProjectName
	config map[string]string
}

func (p *Project) Name() ProjectName {
	return p.name
}

func (p *Project) GetConfig(key string) (string, error) {
	for k, v := range p.config {
		if key == k {
			return v, nil
		}
	}
	return "", errors.New("project config not found: " + key)
}

// GetConfigs returns a clone of project configurations
func (p *Project) GetConfigs() map[string]string {
	confs := make(map[string]string, len(p.config))
	for k, v := range p.config {
		confs[k] = v
	}
	return confs
}

func NewProject(name string, config map[string]string) (*Project, error) {
	prjName, err := ProjectNameFrom(name)
	if err != nil {
		return nil, err
	}

	return &Project{
		name:   prjName,
		config: config,
	}, nil
}
