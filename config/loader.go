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

// MustLoadProjectConfig load the project specific config (see LoadProjectConfig) with panic
func MustLoadProjectConfig(filepath ...string) *ProjectConfig {
	cfg, err := LoadProjectConfig(filepath...)
	if err != nil {
		panic(err)
	}

	return cfg
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

// MustLoadServerConfig load the server specific config (see LoadServerConfig) with panic
func MustLoadServerConfig(filepath ...string) *ServerConfig {
	cfg, err := LoadServerConfig(filepath...)
	if err != nil {
		panic(err)
	}

	return cfg
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

// Validate validate the config as an input. If not valid, it returns error
func Validate(conf interface{}) error {
	switch c := conf.(type) {
	case ProjectConfig:
		return validateProjectConfig(c)
	case ServerConfig:
		return validateServerConfig(c)
	}
	return errors.New("error")
}

func validateProjectConfig(conf ProjectConfig) error {
	// implement this
	return nil
}

func validateServerConfig(conf ServerConfig) error {
	// implement this
	return nil
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

// type LoadConfigFunc func(interface{}, afero.Fs, ...string) error

// // LoadOptimusConfig Load configuration file from following paths
// // ./
// // <exec>/
// // ~/.optimus/
// // Namespaces will be loaded only from current project ./
// func LoadOptimusConfig(dirPaths ...string) (*Optimus, error) {
// 	fs := afero.NewReadOnlyFs(afero.NewOsFs())

// 	var targetPath string
// 	if len(dirPaths) > 0 {
// 		targetPath = dirPaths[0]
// 	} else {
// 		currPath, err := os.Getwd()
// 		if err != nil {
// 			return nil, fmt.Errorf("error getting current work directory path: %w", err)
// 		}
// 		targetPath = currPath
// 	}

// 	optimus := Optimus{}
// 	if err := loadConfig(&optimus, fs, targetPath); err != nil {
// 		return nil, errors.New("error loading config")
// 	}
// 	if err := validateNamespaceDuplication(&optimus); err != nil {
// 		return nil, err
// 	}
// 	return &optimus, nil
// }

// func validateNamespaceDuplication(optimus *Optimus) error {
// 	nameToAppearance := make(map[string]int)
// 	for _, namespace := range optimus.Namespaces {
// 		nameToAppearance[namespace.Name]++
// 	}
// 	var duplicateNames []string
// 	for name, appearance := range nameToAppearance {
// 		if appearance > 1 {
// 			duplicateNames = append(duplicateNames, name)
// 		}
// 	}
// 	if len(duplicateNames) > 0 {
// 		return fmt.Errorf("namespaces [%s] are duplicate", strings.Join(duplicateNames, ", "))
// 	}
// 	return nil
// }
