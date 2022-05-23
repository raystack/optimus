package factory

import (
	"fmt"

	"github.com/odpf/optimus/extension/model"
)

// ParseRegistry is the registry for all parsers defined by each provider
var ParseRegistry []model.Parser

// NewClientRegistry stores all client initializer defined by each provider
var NewClientRegistry = &NewClientFactory{}

// NewClientFactory is a factory to store client initializer
type NewClientFactory struct {
	registry map[string]model.NewClient
}

// Add adds client initializer based on provider
func (c *NewClientFactory) Add(provider string, newClient model.NewClient) error {
	if provider == "" {
		return model.ErrEmptyProvider
	}
	if newClient == nil {
		return fmt.Errorf("[%s] newClient is nil", provider)
	}
	if c.registry == nil {
		c.registry = make(map[string]model.NewClient)
	}
	if c.registry[provider] != nil {
		return fmt.Errorf("[%s] is already registered", provider)
	}
	c.registry[provider] = newClient
	return nil
}

// Get gets client initializer for a specified provider
func (c *NewClientFactory) Get(provider string) (model.NewClient, error) {
	if provider == "" {
		return nil, model.ErrEmptyProvider
	}
	if c.registry == nil {
		c.registry = make(map[string]model.NewClient)
	}
	if c.registry[provider] == nil {
		return nil, fmt.Errorf("[%s] is not registered", provider)
	}
	return c.registry[provider], nil
}
