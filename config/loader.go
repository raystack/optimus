package config

import (
	"os"
	"path/filepath"

	"github.com/odpf/salt/config"
	"github.com/pkg/errors"
)

var (
	ErrFailedToRead = "unable to read optimus config file %v (%s)"
	FileName        = ".optimus"
	FileExtension   = "yaml"
)

// InitOptimus Load configuration file from following paths
// ./
// <exec>/
// ~/.optimus/
func InitOptimus() (*Optimus, error) {
	var o Optimus

	currPath, err := os.Getwd()
	if err != nil {
		return nil, errors.Wrapf(err, "error getting current work directory path")
	}
	execPath, err := os.Executable()
	if err != nil {
		return nil, errors.Wrapf(err, "error getting the executable path")
	}
	currentHomeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, errors.Wrapf(err, "error getting the home directory")
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
		return nil, errors.Wrapf(err, "error loading config")
	}

	return &o, nil
}
