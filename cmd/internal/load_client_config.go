package internal

import (
	"errors"

	saltConfig "github.com/odpf/salt/config"

	"github.com/odpf/optimus/config"
)

func LoadOptionalConfig(configFilePath string) (conf *config.ClientConfig, err error) {
	// TODO: find a way to load the config in one place
	conf, err = config.LoadClientConfig(configFilePath)
	if err != nil && errors.As(err, &saltConfig.ConfigFileNotFoundError{}) {
		err = nil
	}
	return
}
