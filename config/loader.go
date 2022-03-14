package config

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/odpf/salt/config"
	"github.com/spf13/afero"
	"github.com/spf13/viper"
)

const (
	ErrFailedToRead = "unable to read optimus config file %v (%s)"
	FileName        = ".optimus"
	FileExtension   = "yaml"
)

type LoadConfigFunc func(interface{}, afero.Fs, ...string) error

// LoadOptimusConfig Load configuration file from following paths
// ./
// <exec>/
// ~/.optimus/
// Namespaces will be loaded only from current project ./
func LoadOptimusConfig(dirPaths ...string) (*Optimus, error) {
	fs := afero.NewReadOnlyFs(afero.NewOsFs())

	var targetPaths []string
	currPath, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("error getting current work directory path: %w", err)
	}
	if len(dirPaths) > 0 {
		targetPaths = dirPaths
	} else {
		execPath, err := os.Executable()
		if err != nil {
			return nil, errors.New("error getting the executable path")
		}
		currentHomeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, errors.New("error getting the home directory")
		}
		optimusDir := filepath.Join(currentHomeDir, ".optimus")
		targetPaths = []string{currPath, filepath.Dir(execPath), optimusDir}
	}

	optimus := Optimus{}
	if err := loadConfig(&optimus, fs, targetPaths...); err != nil {
		return nil, errors.New("error loading config")
	}

	// Load namespaces config
	namespaces, err := LoadNamespacesConfig(currPath)
	if err != nil {
		return nil, errors.New("error loading namespaces config")
	}
	optimus.Namespaces = namespaces
	return &optimus, nil
}

// LoadNamespacesConfig loads namespace config from 1 level deep of project directory
// |_ .optimus.yaml -> project
// |_ ns1
//    |_ .optimus.yaml -> namespaces 1
// |_ ns2
//    |_ .optimus.yaml -> namespaces 2
// |_ ...
func LoadNamespacesConfig(currPath string) (map[string]*Namespace, error) {
	fs := afero.NewReadOnlyFs(afero.NewOsFs())
	fileInfos, err := afero.ReadDir(fs, currPath)
	if err != nil {
		return nil, err
	}
	output := make(map[string]*Namespace)
	for _, fileInfo := range fileInfos {
		// check if .optimus.yaml exist
		dirPath := path.Join(currPath, fileInfo.Name())
		filePath := path.Join(dirPath, FileName+"."+FileExtension)
		if _, err := fs.Stat(filePath); os.IsNotExist(err) {
			continue
		}

		// load namespace config
		// TODO: find a proper way to load namespace value without introducing additional fields
		optimus := struct {
			Version   string    `mapstructure:"version"`
			Namespace Namespace `mapstructure:"namespace"`
		}{}
		if err := loadConfig(&optimus, fs, dirPath); err != nil {
			return nil, err
		}
		namespace := optimus.Namespace

		if namespace.Name == "" {
			continue
		}
		if output[namespace.Name] != nil {
			fmt.Printf("warning! namespace [%s] from [%s] is already used\n", namespace.Name, filePath)
			continue
		}

		// assigning absolute path for job & datastore
		namespace.Job.Path = path.Join(currPath, fileInfo.Name(), namespace.Job.Path)
		for i, d := range namespace.Datastore {
			namespace.Datastore[i].Path = path.Join(currPath, fileInfo.Name(), d.Path)
		}
		output[namespace.Name] = &namespace
	}
	if len(output) == 0 {
		output = nil
	}
	return output, nil
}

func loadConfig(cfg interface{}, fs afero.Fs, paths ...string) error {
	// getViperWithDefault + SetFs
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.SetFs(fs)

	opts := []config.LoaderOption{
		config.WithViper(v),
		config.WithName(FileName),
		config.WithType(FileExtension),
		config.WithEnvPrefix("OPTIMUS"),
		config.WithEnvKeyReplacer(".", "_"),
	}
	for _, path := range paths {
		opts = append(opts, config.WithPath(path))
	}

	l := config.NewLoader(opts...)
	return l.Load(cfg)
}
