package config

import (
	"errors"
	"strings"

	"github.com/odpf/salt/config"
	"github.com/spf13/afero"
	"github.com/spf13/viper"
)

const (
	ErrFailedToRead      = "unable to read optimus config file %v (%s)"
	DefaultFilename      = ".optimus"
	DefaultFileExtension = "yaml"
)

var (
	filename      = DefaultFilename
	fileExtension = DefaultFileExtension // ASK: are we providing file extension other than yaml?
)

// MustLoadProjectConfig load the project specific config (see LoadProjectConfig) with panic
func MustLoadProjectConfig(path ...string) *ProjectConfig {
	// implement this
	return nil
}

// LoadProjectConfig load the project specific config from these locations:
// 1. env var. eg. OPTIMUS_PROJECT, OPTIMUS_NAMESPACES, etc
// 2. path. ./optimus <client_command> -c "path/to/config/optimus.yaml"
// 3. current dir. Optimus will look at current directory if there's optimus.yaml there, use it
func LoadProjectConfig(path ...string) (*ProjectConfig, error) {
	// implement this
	return nil, nil
}

// MustLoadServerConfig load the server specific config (see LoadServerConfig) with panic
func MustLoadServerConfig(path ...string) *ServerConfig {
	// implement this
	return nil
}

// LoadServerConfig load the server specific config from these locations:
// 1. env var. eg. OPTIMUS_SERVE_PORT, etc
// 2. path. ./optimus <server_command> -c "path/to/config.yaml"
// 3. executable binary location
// 4. home dir
func LoadServerConfig(path ...string) (*ServerConfig, error) {
	// implement this
	return nil, nil
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

func loadConfig(cfg interface{}, fs afero.Fs, dirPaths ...string) error {
	// getViperWithDefault + SetFs
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.SetFs(fs)

	opts := []config.LoaderOption{
		config.WithViper(v),
		config.WithName(filename),
		config.WithType(fileExtension),
		config.WithEnvPrefix("OPTIMUS"),
		config.WithEnvKeyReplacer(".", "_"),
	}

	for _, path := range dirPaths {
		opts = append(opts, config.WithPath(path))
	}

	l := config.NewLoader(opts...)
	return l.Load(cfg)
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
