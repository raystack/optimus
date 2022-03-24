package config

import (
	"fmt"
	"os"

	"github.com/odpf/salt/config"
	"github.com/spf13/afero"

	"github.com/spf13/viper"
)

const (
	ErrFailedToRead      = "unable to read optimus config file %v (%s)"
	DefaultFilename      = "optimus"
	DefaultFileExtension = "yaml"
	DefaultEnvPrefix     = "OPTIMUS"
	EmptyPath            = ""
)

var (
	FS       = afero.NewReadOnlyFs(afero.NewOsFs())
	currPath string
	execPath string
	homePath string
)

func init() {
	p, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	currPath = p

	p, err = os.Executable()
	if err != nil {
		panic(err)
	}
	execPath = p

	p, err = os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	homePath = p
}

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
		opts = append(opts, config.WithPath(currPath))
	}

	// load the config
	l := config.NewLoader(opts...)
	if err := l.Load(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// LoadServerConfig load the server specific config from these locations:
// 1. filepath. ./optimus <server_command> -c "path/to/config.yaml"
// 2. env var. eg. OPTIMUS_SERVE_PORT, etc
// 3. executable binary location
// 4. home dir
func LoadServerConfig(filePath string) (*ServerConfig, error) {
	cfg := &ServerConfig{}

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
		// load opt from env var
		opts = append(opts, config.WithEnvPrefix(DefaultEnvPrefix), config.WithEnvKeyReplacer(".", "_"))

		// load opt from exec & home directory
		opts = append(opts, config.WithPath(execPath), config.WithPath(homePath))
	}

	// load the config
	l := config.NewLoader(opts...)
	if err := l.Load(cfg); err != nil {
		return nil, err
	}

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
