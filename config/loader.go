package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/odpf/salt/config"
	"gopkg.in/yaml.v2"
)

const (
	ErrFailedToRead = "unable to read optimus config file %v (%s)"
	FileName        = ".optimus"
	FileExtension   = "yaml"
)

// LoadOptimusConfig Load configuration file from following paths
// ./
// <exec>/
// ~/.optimus/
func LoadOptimusConfig() (*Optimus, error) {
	var o Optimus
	currPath, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("error getting current work directory path: %w", err)
	}
	execPath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("error getting the executable path: %w", err)
	}
	currentHomeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("error getting the home directory: %w", err)
	}
	optimusDir := filepath.Join(currentHomeDir, ".optimus")
	l := config.NewLoader(
		config.WithName(FileName),
		config.WithType(FileExtension),
		config.WithPath(currPath),
		config.WithPath(filepath.Dir(execPath)),
		config.WithPath(optimusDir),
		config.WithEnvPrefix("OPTIMUS"),
		config.WithEnvKeyReplacer(".", "_"),
	)

	if err := l.Load(&o); err != nil {
		return nil, fmt.Errorf("error loading config: %w", err)
	}
	return &o, nil
}

func LoadNamespaceConfig() (map[string]*Namespace, error) {
	currPath, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("error getting current work directory path: %w", err)
	}
	infos, err := ioutil.ReadDir(currPath)
	if err != nil {
		return nil, err
	}
	output := make(map[string]*Namespace)
	for _, info := range infos {
		dirPath := info.Name()
		filePath := path.Join(currPath, dirPath, FileName, FileExtension)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			continue
		}
		content, err := ioutil.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("error reading config from: %s. %w", filePath, err)
		}
		var namespace Namespace
		if err := yaml.Unmarshal(content, &namespace); err != nil {
			return nil, fmt.Errorf("error unmarshalling config: %s. %w", filePath, err)
		}
		if output[namespace.Name] != nil {
			fmt.Printf("warning! namespace [%s] from [%s] is already used", namespace.Name, filePath)
			continue
		}
		if namespace.Job.Path != "" {
			namespace.Job.Path = path.Join(currPath, dirPath, namespace.Job.Path)
		}
		for i, d := range namespace.Datastore {
			if d.Path != "" {
				namespace.Datastore[i].Path = path.Join(currPath, dirPath, d.Path)
			}
		}
		output[namespace.Name] = &namespace
	}
	return output, nil
}
