package config

import (
	"errors"
	"os"

	"github.com/odpf/salt/config"
	"github.com/spf13/afero"

	"github.com/spf13/viper"
)

const (
	ErrFailedToRead      = "unable to read optimus config file %v (%s)"
	DefaultFilename      = ".optimus"
	DefaultFileExtension = "yaml"
	DefaultEnvPrefix     = "OPTIMUS"
)

var (
	filename      = DefaultFilename
	fileExtension = DefaultFileExtension // ASK: are we providing file extension other than yaml?
	envPrefix     = DefaultEnvPrefix
	currPath      string
	execPath      string
	homePath      string
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

// LoadProjectConfig load the project specific config from these locations:
// 1. env var. eg. OPTIMUS_PROJECT, OPTIMUS_NAMESPACES, etc
// 2. filepath. ./optimus <client_command> -c "path/to/config/optimus.yaml"
// 3. current dir. Optimus will look at current directory if there's optimus.yaml there, use it
func LoadProjectConfig(filepath ...string) (*ProjectConfig, error) {
	fs := afero.NewReadOnlyFs(afero.NewOsFs())
	return loadProjectConfigFs(fs, filepath...)
}

func loadProjectConfigFs(fs afero.Fs, filepath ...string) (*ProjectConfig, error) {
	cfg := &ProjectConfig{}

	if len(filepath) == 0 {
		filepath = append(filepath, "")
	}

	err := loadConfig(cfg, fs, filepath[0], currPath)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// LoadServerConfig load the server specific config from these locations:
// 1. env var. eg. OPTIMUS_SERVE_PORT, etc
// 2. filepath. ./optimus <server_command> -c "path/to/config.yaml"
// 3. executable binary location
// 4. home dir
func LoadServerConfig(filepath ...string) (*ServerConfig, error) {
	fs := afero.NewReadOnlyFs(afero.NewOsFs())
	return loadServerConfigFs(fs, filepath...)
}

func loadServerConfigFs(fs afero.Fs, filepath ...string) (*ServerConfig, error) {
	cfg := &ServerConfig{}

	if len(filepath) == 0 {
		filepath = append(filepath, "")
	}

	err := loadConfig(cfg, fs, filepath[0], execPath, homePath)
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func loadConfig(cfg interface{}, fs afero.Fs, paths ...string) error {
	// getViperWithDefault + SetFs
	v := viper.New()
	v.SetFs(fs)

	opts := []config.LoaderOption{
		config.WithViper(v),
		config.WithName(filename),
		config.WithType(fileExtension),
		config.WithEnvPrefix(envPrefix),
		config.WithEnvKeyReplacer(".", "_"),
	}

	if len(paths) > 0 {
		fpath := paths[0]
		dirPaths := paths[1:]

		if fpath != "" {
			if err := validateFilepath(fs, fpath); err != nil {
				return err
			}
			opts = append(opts, config.WithFile(fpath))
		}

		for _, path := range dirPaths {
			opts = append(opts, config.WithPath(path))
		}
	}

	l := config.NewLoader(opts...)
	return l.Load(cfg)
}

func validateFilepath(fs afero.Fs, fpath string) error {
	f, err := fs.Stat(fpath)
	if err != nil {
		return err
	}
	if !f.Mode().IsRegular() {
		return errors.New("not a file")
	}
	return nil
}
