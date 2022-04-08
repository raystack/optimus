package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/odpf/salt/config"
	"github.com/spf13/afero"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	ErrFailedToRead       = "unable to read optimus config file %v (%s)"
	DefaultFilename       = "optimus"
	DefaultConfigFilename = "config" // default file name for server config
	DefaultFileExtension  = "yaml"
	DefaultEnvPrefix      = "OPTIMUS"
	EmptyPath             = ""
)

var (
	FS         = afero.NewReadOnlyFs(afero.NewOsFs())
	EmptyFlags = &pflag.FlagSet{}
	currPath   string
	execPath   string
)

//nolint:gochecknoinits
func init() { // TODO: move paths initialization outside init()
	p, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	currPath = p

	p, err = os.Executable()
	if err != nil {
		panic(err)
	}
	execPath = filepath.Dir(p)
}

// LoadClientConfig load the project specific config from these locations:
// 1. flags. eg ./optimus <client_command> --project.name project1
// 2. filepath. ./optimus <client_command> -c "path/to/config/optimus.yaml"
// 3. current dir. Optimus will look at current directory if there's optimus.yaml there, use it
func LoadClientConfig(filePath string, flags *pflag.FlagSet) (*ClientConfig, error) {
	cfg := &ClientConfig{}

	// getViperWithDefault + SetFs
	v := viper.New()
	v.SetFs(FS)

	// bind with flags
	setPFlagsNormalizer(flags)
	if err := v.BindPFlags(flags); err != nil {
		return nil, err
	}

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

	cfg.Log.Level = LogLevel(strings.ToUpper(string(cfg.Log.Level)))

	return cfg, nil
}

// LoadServerConfig load the server specific config from these locations:
// 1. flags. eg ./optimus <server_command> --serve.port 8000
// 2. filepath. ./optimus <server_command> -c "path/to/config.yaml"
// 3. env var. eg. OPTIMUS_SERVE_PORT, etc
// 4. executable binary location
func LoadServerConfig(filePath string, flags *pflag.FlagSet) (*ServerConfig, error) {
	cfg := &ServerConfig{}

	// getViperWithDefault + SetFs
	v := viper.New()
	v.SetFs(FS)

	// bind with flags
	setPFlagsNormalizer(flags)
	if err := v.BindPFlags(flags); err != nil {
		return nil, err
	}

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
		// load opt from env var
		opts = append(opts, config.WithEnvPrefix(DefaultEnvPrefix), config.WithEnvKeyReplacer(".", "_"))

		// load opt from exec
		opts = append(opts, config.WithPath(execPath))
	}

	// load the config
	l := config.NewLoader(opts...)
	if err := l.Load(cfg); err != nil {
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

func setPFlagsNormalizer(flags *pflag.FlagSet) {
	// normalize with the pflag names with replacer
	normalizeFunc := flags.GetNormalizeFunc()
	flags.SetNormalizeFunc(func(fs *pflag.FlagSet, name string) pflag.NormalizedName {
		result := normalizeFunc(fs, name)
		name = strings.ReplaceAll(string(result), "-", ".")
		return pflag.NormalizedName(name)
	})
}
