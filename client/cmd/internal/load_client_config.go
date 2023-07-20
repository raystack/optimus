package internal

import (
	"errors"

	saltConfig "github.com/raystack/salt/config"

	"github.com/raystack/optimus/config"
)

// TODO: need to do refactor for proper file naming
func LoadOptionalConfig(configFilePath string) (conf *config.ClientConfig, err error) {
	conf, err = config.LoadClientConfig(configFilePath)
	if err != nil && errors.As(err, &saltConfig.ConfigFileNotFoundError{}) {
		err = nil
	}
	return
}
