package exd

import (
	"errors"
	"fmt"
)

var ParseRegistry []Parser

var NewClientRegistry = &NewClientFactory{}

type NewClientFactory struct {
	registry map[string]NewClient
}

func (c *NewClientFactory) Add(providerName string, newClient NewClient) error {
	if providerName == "" {
		return errors.New("provider name is empty")
	}
	if newClient == nil {
		return fmt.Errorf("[%s] newClient is nil", providerName)
	}
	if c.registry == nil {
		c.registry = make(map[string]NewClient)
	}
	if c.registry[providerName] != nil {
		return fmt.Errorf("[%s] is already registered", providerName)
	}
	c.registry[providerName] = newClient
	return nil
}

func (c *NewClientFactory) Get(providerName string) (NewClient, error) {
	if providerName == "" {
		return nil, errors.New("provider name is empty")
	}
	if c.registry == nil {
		c.registry = make(map[string]NewClient)
	}
	if c.registry[providerName] == nil {
		return nil, fmt.Errorf("[%s] is not registered", providerName)
	}
	return c.registry[providerName], nil
}
