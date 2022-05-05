package exd

import (
	"fmt"
)

// ParseRegistry is the registry for all parsers defined by each provider
var ParseRegistry []Parser

// NewClientRegistry stores all client initializer defined by each provider
var NewClientRegistry = &NewClientFactory{}

// NewClientFactory is a factory to store client initializer
type NewClientFactory struct {
	registry map[string]NewClient
}

// Add adds client initializer based on provider
func (c *NewClientFactory) Add(provider string, newClient NewClient) error {
	if provider == "" {
		return ErrEmptyProvider
	}
	if newClient == nil {
		return fmt.Errorf("[%s] newClient is nil", provider)
	}
	if c.registry == nil {
		c.registry = make(map[string]NewClient)
	}
	if c.registry[provider] != nil {
		return fmt.Errorf("[%s] is already registered", provider)
	}
	c.registry[provider] = newClient
	return nil
}

// Get gets client initializer for a specified provider
func (c *NewClientFactory) Get(provider string) (NewClient, error) {
	if provider == "" {
		return nil, ErrEmptyProvider
	}
	if c.registry == nil {
		c.registry = make(map[string]NewClient)
	}
	if c.registry[provider] == nil {
		return nil, fmt.Errorf("[%s] is not registered", provider)
	}
	return c.registry[provider], nil
}
