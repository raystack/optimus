package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/goto/salt/config"
	"github.com/spf13/afero"
	"github.com/spf13/viper"
)

const (
	DefaultFilename       = "optimus.yaml"
	DefaultConfigFilename = "config.yaml" // default file name for server config
	DefaultFileExtension  = "yaml"
	DefaultEnvPrefix      = "OPTIMUS"
	EmptyPath             = ""
)

var FS = afero.NewReadOnlyFs(afero.NewOsFs())

// LoadClientConfig load the project specific config from these locations:
// 1. filepath. ./optimus <client_command> -c "path/to/config/optimus.yaml"
// 2. current dir. Optimus will look at current directory if there's optimus.yaml there, use it
func LoadClientConfig(filePath string) (*ClientConfig, error) {
	cfg := &ClientConfig{}

	// getViperWithDefault + SetFs
	v := viper.New()
	v.SetFs(FS)

	opts := []config.LoaderOption{
		config.WithViper(v),
		config.WithName(DefaultFilename),
		config.WithType(DefaultFileExtension),
	}

	// load opt from filepath if exist
	if filePath != EmptyPath {
		if err := validateFilepath(FS, filePath); err != nil {
			return nil, err // if filepath not valid, returns err
		}
		opts = append(opts, config.WithFile(filePath))
	} else {
		// load opt from current directory
		currPath, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		opts = append(opts, config.WithPath(currPath))
	}

	// load the config
	l := config.NewLoader(opts...)
	if err := l.Load(cfg); err != nil {
		return nil, err
	}

	cfg.Log.Level = LogLevel(strings.ToUpper(string(cfg.Log.Level)))

	return cfg, nil
}

// LoadServerConfig load the server specific config from these locations:
// 1. filepath. ./optimus <server_command> -c "path/to/config.yaml"
// 2. env var. eg. OPTIMUS_SERVE_PORT, etc
// 3. executable binary location
func LoadServerConfig(filePath string) (*ServerConfig, error) {
	cfg := &ServerConfig{}

	// getViperWithDefault + SetFs
	v := viper.New()
	v.SetFs(FS)

	opts := []config.LoaderOption{
		config.WithViper(v),
		config.WithName(DefaultConfigFilename),
		config.WithType(DefaultFileExtension),
	}

	// load opt from filepath if exist
	if filePath != EmptyPath {
		if err := validateFilepath(FS, filePath); err != nil {
			return nil, err // if filepath not valid, returns err
		}
		opts = append(opts, config.WithFile(filePath))
	} else {
		// load opt from exec
		p, err := os.Executable()
		if err != nil {
			return nil, err
		}
		execPath := filepath.Dir(p)
		opts = append(opts, config.WithPath(execPath))
	}
	// load opt from env var
	opts = append(opts, config.WithEnvPrefix(DefaultEnvPrefix), config.WithEnvKeyReplacer(".", "_"))

	// load the config
	l := config.NewLoader(opts...)
	if err := l.Load(cfg); err != nil && !errors.As(err, &config.ConfigFileNotFoundError{}) {
		return nil, err
	}

	cfg.Log.Level = LogLevel(strings.ToUpper(string(cfg.Log.Level)))

	return cfg, nil
}

func validateFilepath(fs afero.Fs, fpath string) error {
	f, err := fs.Stat(fpath)
	if err != nil {
		return err
	}
	if !f.Mode().IsRegular() {
		return fmt.Errorf("%s not a file", fpath)
	}
	return nil
}
