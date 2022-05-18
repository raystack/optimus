package extension_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/extension"
)

type NewClientFactoryTestSuite struct {
	suite.Suite
}

func (n *NewClientFactoryTestSuite) TestAdd() {
	n.Run("should return error if provider is empty", func() {
		var provider string
		newClientFactory := &extension.NewClientFactory{}
		newClient := func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return nil, nil
		}

		actualErr := newClientFactory.Add(provider, newClient)

		n.Error(actualErr)
	})

	n.Run("should return error if client initializer is nil", func() {
		provider := "test_provider"
		newClientFactory := &extension.NewClientFactory{}
		var newClient extension.NewClient

		actualErr := newClientFactory.Add(provider, newClient)

		n.Error(actualErr)
	})

	n.Run("should return error if client initializer is already registered", func() {
		provider := "test_provider"
		newClientFactory := &extension.NewClientFactory{}
		newClient := func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return nil, nil
		}

		actualFirstErr := newClientFactory.Add(provider, newClient)
		actualSecondErr := newClientFactory.Add(provider, newClient)

		n.NoError(actualFirstErr)
		n.Error(actualSecondErr)
	})
}

func (n *NewClientFactoryTestSuite) TestGet() {
	n.Run("should return nil and error if provider is empty", func() {
		registeredProvider := "test_provider"
		newClientFactory := &extension.NewClientFactory{}
		newClient := func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return nil, nil
		}
		if err := newClientFactory.Add(registeredProvider, newClient); err != nil {
			panic(err)
		}

		var testProvider string

		actualNewClient, actualErr := newClientFactory.Get(testProvider)

		n.Nil(actualNewClient)
		n.Error(actualErr)
	})

	n.Run("should return nil and error if provider is not registered", func() {
		newClientFactory := &extension.NewClientFactory{}

		testProvider := "test_provider"

		actualNewClient, actualErr := newClientFactory.Get(testProvider)

		n.Nil(actualNewClient)
		n.Error(actualErr)
	})

	n.Run("should return client initializer and nil if no error is encountered", func() {
		registeredProvider := "test_provider"
		newClientFactory := &extension.NewClientFactory{}
		newClient := func(ctx context.Context, httpDoer extension.HTTPDoer) (extension.Client, error) {
			return nil, nil
		}
		if err := newClientFactory.Add(registeredProvider, newClient); err != nil {
			panic(err)
		}

		testProvider := "test_provider"

		actualNewClient, actualErr := newClientFactory.Get(testProvider)

		n.NotNil(actualNewClient)
		n.NoError(actualErr)
	})
}

func TestNewClientFactory(t *testing.T) {
	suite.Run(t, &NewClientFactoryTestSuite{})
}
