package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/knadh/koanf/providers/confmap"

	"github.com/pkg/errors"

	"github.com/knadh/koanf/providers/env"

	"github.com/knadh/koanf/providers/file"
	"github.com/spf13/afero"

	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/yaml"
)

var (
	ErrFailedToRead = "unable to read optimus config file %v (%s)"
	FileName        = ".optimus"
	FileExtension   = "yaml"
)

// Load configuration file from following paths
// ./
// <exec>/
// ~/.config/
// ~/.optimus/
func InitOptimus() (*Optimus, error) {
	configuration := &Optimus{
		k:      koanf.New("."),
		parser: yaml.Parser(),
	}

	configDirs := []string{}
	if p, err := os.Getwd(); err == nil {
		configDirs = append(configDirs, p)
	}
	if execPath, err := os.Executable(); err == nil {
		configDirs = append(configDirs, filepath.Dir(execPath))
	}
	if currentHomeDir, err := os.UserHomeDir(); err == nil {
		configDirs = append(configDirs, filepath.Join(currentHomeDir, ".config"))
		configDirs = append(configDirs, filepath.Join(currentHomeDir, ".optimus"))
	}

	// load defaults
	if err := configuration.k.Load(confmap.Provider(map[string]interface{}{
		KeyLogLevel:                     "info",
		KeyServePort:                    9100,
		KeyServeHost:                    "0.0.0.0",
		KeyServeDBMaxOpenConnection:     10,
		KeyServeDBMaxIdleConnection:     5,
		KeyServeMetadataKafkaJobTopic:   "resource_optimus_job_log",
		KeyServeMetadataKafkaBatchSize:  50,
		KeyServeMetadataWriterBatchSize: 50,
		KeySchedulerName:                "airflow2",
	}, "."), nil); err != nil {
		return nil, errors.Wrap(err, "k.Load: error loading config defaults")
	}

	// Load yaml config
	fs := afero.NewOsFs()
	pathUsed := ""
	for _, path := range configDirs {
		path = filepath.Join(path, fmt.Sprintf("%s.%s", FileName, FileExtension))
		if ok, err := exists(fs, path); !ok || err != nil {
			continue
		}
		if err := configuration.k.Load(file.Provider(path), configuration.parser); err != nil {
			return nil, errors.Wrapf(err, "k.Load: error loading config from %s", path)
		}
		pathUsed = path
		break
	}

	// load envs
	if err := configuration.k.Load(env.Provider("OPTIMUS_", ".", func(s string) string {
		return strings.Replace(strings.ToLower(
			strings.TrimPrefix(s, "OPTIMUS_")), "_", ".", -1)
	}), nil); err != nil {
		return nil, errors.Wrap(err, "k.Load: error loading config from env")
	}

	if pathUsed != "" && strings.ToLower(configuration.GetLog().Level) == "debug" {
		fmt.Printf("configuration used at %s out of %v\n", pathUsed, configDirs)
	}
	return configuration, nil
}

func exists(fs afero.Fs, path string) (bool, error) {
	stat, err := fs.Stat(path)
	if err == nil {
		return !stat.IsDir(), nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
