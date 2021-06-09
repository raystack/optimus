package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

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

	k      = koanf.New(".")
	parser = yaml.Parser()
)

// Load configuration file from following paths
// ./
// <exec>/
// ~/.config/
// ~/.optimus/
func Init() (*Optimus, error) {
	configuration := &Optimus{}
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

	// Load yaml config
	fs := afero.NewOsFs()
	pathUsed := ""
	for _, path := range configDirs {
		path = filepath.Join(path, fmt.Sprintf("%s.%s", FileName, FileExtension))
		if ok, err := exists(fs, path); !ok || err != nil {
			continue
		}
		if err := k.Load(file.Provider(path), parser); err != nil {
			return nil, errors.Wrapf(err, "k.Load: error loading config from %s", path)
		}
		pathUsed = path
		break
	}

	// load envs
	if err := k.Load(env.Provider("OPTIMUS_", ".", func(s string) string {
		return strings.Replace(strings.ToLower(
			strings.TrimPrefix(s, "OPTIMUS_")), "_", ".", -1)
	}), nil); err != nil {
		return nil, errors.Wrap(err, "k.Load: error loading config from env")
	}

	if err := k.Unmarshal("", &configuration); err != nil {
		return nil, errors.Wrap(err, "k.Unmarshal: error unmarshalling config")
	}

	configuration = setDefaults(configuration)
	if strings.ToLower(configuration.Log.Level) == "debug" {
		fmt.Printf("configuration used at %s out of %v\n", pathUsed, configDirs)
	}
	return configuration, nil
}

func setDefaults(conf *Optimus) *Optimus {
	if conf.Log.Level == "" {
		conf.Log.Level = "info"
	}
	if conf.Serve.Port == 0 {
		conf.Serve.Port = 9100
	}
	if conf.Serve.Host == "" {
		conf.Serve.Host = "0.0.0.0"
	}
	if conf.Serve.Port == 0 {
		conf.Serve.Port = 9100
	}
	if conf.Serve.DB.MaxOpenConnection == 0 {
		conf.Serve.DB.MaxOpenConnection = 10
	}
	if conf.Serve.DB.MaxIdleConnection == 0 {
		conf.Serve.DB.MaxIdleConnection = 5
	}
	if conf.Serve.Metadata.KafkaJobTopic == "" {
		conf.Serve.Metadata.KafkaJobTopic = "resource_optimus_job_log"
	}
	if conf.Serve.Metadata.KafkaBatchSize == 0 {
		conf.Serve.Metadata.KafkaBatchSize = 50
	}
	if conf.Serve.Metadata.KafkaBatchSize == 0 {
		conf.Serve.Metadata.KafkaBatchSize = 50
	}
	if conf.Serve.Metadata.WriterBatchSize == 0 {
		conf.Serve.Metadata.WriterBatchSize = 50
	}

	return conf
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
