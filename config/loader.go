package config

import (
	"errors"
	"fmt"
	"os"
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

	var targetPath string
	if len(dirPaths) > 0 {
		targetPath = dirPaths[0]
	} else {
		currPath, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("error getting current work directory path: %w", err)
		}
		targetPath = currPath
	}

	optimus := Optimus{}
	if err := loadConfig(&optimus, fs, targetPath); err != nil {
		return nil, errors.New("error loading config")
	}
	if err := validateNamespaceDuplication(&optimus); err != nil {
		return nil, err
	}
	return &optimus, nil
}

func validateNamespaceDuplication(optimus *Optimus) error {
	nameToAppearance := make(map[string]int)
	for _, namespace := range optimus.Namespaces {
		nameToAppearance[namespace.Name]++
	}
	var duplicateNames []string
	for name, appearance := range nameToAppearance {
		if appearance > 1 {
			duplicateNames = append(duplicateNames, name)
		}
	}
	if len(duplicateNames) > 0 {
		return fmt.Errorf("namespaces [%s] are duplicate", strings.Join(duplicateNames, ", "))
	}
	return nil
}

func loadConfig(cfg interface{}, fs afero.Fs, dirPath string) error {
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
		config.WithPath(dirPath),
	}

	l := config.NewLoader(opts...)
	return l.Load(cfg)
}
