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
// ~/.config/
// ~/.optimus/
func InitOptimus() (*Optimus, error) {
	var o Optimus

	currPath, _ := os.Getwd()
	execPath, _ := os.Executable()
	currentHomeDir, _ := os.UserHomeDir()
	configDir := filepath.Join(currentHomeDir, ".config")
	optimusDir := filepath.Join(currentHomeDir, ".optimus")

	l := config.NewLoader(
		config.WithName(FileName),
		config.WithType(FileExtension),
		config.WithPath(currPath),
		config.WithPath(filepath.Dir(execPath)),
		config.WithPath(configDir),
		config.WithPath(optimusDir),
		config.WithEnvPrefix("OPTIMUS"),
		config.WithEnvKeyReplacer(".", "_"),
	)

	if err := l.Load(&o); err != nil {
		return nil, errors.Wrapf(err, "error loading config")
	}

	return &o, nil
}
