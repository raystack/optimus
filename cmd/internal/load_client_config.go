package internal

import (
	"errors"

	"github.com/odpf/optimus/config"
	saltConfig "github.com/odpf/salt/config"
)

func LoadOptionalConfig(configFilePath string) (*config.ClientConfig, error) {
	// TODO: find a way to load the config in one place
	c, err := config.LoadClientConfig(configFilePath)
	if err != nil {
		if errors.As(err, &saltConfig.ConfigFileNotFoundError{}) {
			return nil, nil
		}
		return nil, err
	}
	return c, nil
}
