package factory

import (
	"fmt"

	"github.com/odpf/optimus/extension/model"
)

// ParseRegistry is the registry for all parsers defined by each provider
var ParseRegistry []model.Parser

// ClientRegistry stores all clients defined by each provider
var ClientRegistry = &ClientFactory{}

// ClientFactory is a factory to store client
type ClientFactory struct {
	registry map[string]model.Client
}

// Add adds client based on provider
func (c *ClientFactory) Add(provider string, newClient model.Client) error {
	if provider == "" {
		return model.ErrEmptyProvider
	}
	if newClient == nil {
		return fmt.Errorf("[%s] newClient is nil", provider)
	}
	if c.registry == nil {
		c.registry = make(map[string]model.Client)
	}
	if c.registry[provider] != nil {
		return fmt.Errorf("[%s] is already registered", provider)
	}
	c.registry[provider] = newClient
	return nil
}

// Get gets client for a specified provider
func (c *ClientFactory) Get(provider string) (model.Client, error) {
	if provider == "" {
		return nil, model.ErrEmptyProvider
	}
	if c.registry == nil {
		c.registry = make(map[string]model.Client)
	}
	if c.registry[provider] == nil {
		return nil, fmt.Errorf("[%s] is not registered", provider)
	}
	return c.registry[provider], nil
}
