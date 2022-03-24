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

// LoadOptimusConfig Load configuration file from following paths (LEGACY)
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
		config.WithName(DefaultFilename),
		config.WithType(DefaultFileExtension),
		config.WithEnvPrefix("OPTIMUS"),
		config.WithEnvKeyReplacer(".", "_"),
		config.WithPath(dirPath),
	}

	l := config.NewLoader(opts...)
	return l.Load(cfg)
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
