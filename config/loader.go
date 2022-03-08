package config

import (
	"fmt"
	"io/ioutil"
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
func LoadOptimusConfig() (*Optimus, error) {
	var fs afero.Fs
	var o Optimus

	fs = afero.NewReadOnlyFs(afero.NewOsFs())
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

	// Load optimus config (server & project)
	if err = LoadConfig(o, fs, currPath, filepath.Dir(execPath), optimusDir); err != nil {
		return nil, fmt.Errorf("error loading config: %w", err)
	}

	// Load namespaces config
	if err = LoadNamespacesConfig(LoadConfig)(o.Namespaces, fs, currPath); err != nil {
		return nil, fmt.Errorf("error loading namespaces config: %w", err)
	}

	return &o, nil
}

// LoadNamespacesConfig loads namespace config from 1 level deep of project directory
// |_ .optimus.yaml -> project
// |_ ns1
//    |_ .optimus.yaml -> namespaces 1
// |_ ns2
//    |_ .optimus.yaml -> namespaces 2
// |_ ...
func LoadNamespacesConfig(f LoadConfigFunc) func(map[string]*Namespace, afero.Fs, string) error {
	return func(namespaces map[string]*Namespace, fs afero.Fs, currPath string) error {
		fileInfos, err := ioutil.ReadDir(currPath)
		if err != nil {
			return err
		}
		for _, fileInfo := range fileInfos {
			// check if .optimus.yaml exist
			filePath := path.Join(currPath, fileInfo.Name(), FileName, FileExtension)
			if _, err := fs.Stat(filePath); os.IsNotExist(err) {
				continue
			}

			// load namespace config
			var namespace Namespace
			if err := f(namespace, fs, filePath); err != nil {
				return err
			}

			// skip conflicted namespace
			if namespaces[namespace.Name] != nil {
				fmt.Printf("warning! namespace [%s] from [%s] is already used", namespace.Name, filePath)
				continue
			}

			// assigning absolute path for job & datastore
			namespace.Job.Path = path.Join(currPath, fileInfo.Name(), namespace.Job.Path)
			for i, d := range namespace.Datastore {
				namespace.Datastore[i].Path = path.Join(currPath, fileInfo.Name(), d.Path)
			}

			// assigning to namespaces map
			namespaces[namespace.Name] = &namespace
		}
		return nil
	}
}

func LoadConfig(cfg interface{}, fs afero.Fs, paths ...string) error {
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
