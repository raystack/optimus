package factory_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/raystack/optimus/client/extension/factory"
	"github.com/raystack/optimus/client/extension/mock"
	"github.com/raystack/optimus/client/extension/model"
)

type ClientFactoryTestSuite struct {
	suite.Suite
}

func (n *ClientFactoryTestSuite) TestAdd() {
	n.Run("should return error if provider is empty", func() {
		var provider string
		clientFactory := &factory.ClientFactory{}
		client := &mock.Client{}

		actualErr := clientFactory.Add(provider, client)

		n.Error(actualErr)
	})

	n.Run("should return error if client is nil", func() {
		provider := "test_provider"
		clientFactory := &factory.ClientFactory{}
		var client model.Client

		actualErr := clientFactory.Add(provider, client)

		n.Error(actualErr)
	})

	n.Run("should return error if client is already registered", func() {
		provider := "test_provider"
		clientFactory := &factory.ClientFactory{}
		client := &mock.Client{}

		actualFirstErr := clientFactory.Add(provider, client)
		actualSecondErr := clientFactory.Add(provider, client)

		n.NoError(actualFirstErr)
		n.Error(actualSecondErr)
	})

	n.Run("should return nil if no error is encountered", func() {
		provider := "test_provider"
		clientFactory := &factory.ClientFactory{}
		client := &mock.Client{}

		actualErr := clientFactory.Add(provider, client)

		n.NoError(actualErr)
	})
}

func (n *ClientFactoryTestSuite) TestGet() {
	n.Run("should return nil and error if provider is empty", func() {
		clientFactory := &factory.ClientFactory{}

		var testProvider string

		actualNewClient, actualErr := clientFactory.Get(testProvider)

		n.Nil(actualNewClient)
		n.Error(actualErr)
	})

	n.Run("should return nil and error if provider is not registered", func() {
		clientFactory := &factory.ClientFactory{}

		testProvider := "test_provider"

		actualNewClient, actualErr := clientFactory.Get(testProvider)

		n.Nil(actualNewClient)
		n.Error(actualErr)
	})

	n.Run("should return client and nil if no error is encountered", func() {
		registeredProvider := "test_provider"
		clientFactory := &factory.ClientFactory{}
		client := &mock.Client{}
		if err := clientFactory.Add(registeredProvider, client); err != nil {
			panic(err)
		}

		testProvider := "test_provider"

		actualNewClient, actualErr := clientFactory.Get(testProvider)

		n.NotNil(actualNewClient)
		n.NoError(actualErr)
	})
}

func TestClientFactory(t *testing.T) {
	suite.Run(t, &ClientFactoryTestSuite{})
}
