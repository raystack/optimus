package exd_test

import (
	"context"
	"testing"

	"github.com/odpf/optimus/ext/exd"
	"github.com/stretchr/testify/suite"
)

type NewClientFactoryTestSuite struct {
	suite.Suite
}

func (n *NewClientFactoryTestSuite) TestAdd() {
	n.Run("should return error if provider name is empty", func() {
		var providerName string
		newClientFactory := &exd.NewClientFactory{}
		newClient := func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return nil, nil
		}

		actualErr := newClientFactory.Add(providerName, newClient)

		n.Error(actualErr)
	})

	n.Run("should return error if client initializer is nil", func() {
		providerName := "test_provider"
		newClientFactory := &exd.NewClientFactory{}
		var newClient exd.NewClient

		actualErr := newClientFactory.Add(providerName, newClient)

		n.Error(actualErr)
	})

	n.Run("should return error if client initializer is already registered", func() {
		providerName := "test_provider"
		newClientFactory := &exd.NewClientFactory{}
		newClient := func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return nil, nil
		}

		actualFirstErr := newClientFactory.Add(providerName, newClient)
		actualSecondErr := newClientFactory.Add(providerName, newClient)

		n.NoError(actualFirstErr)
		n.Error(actualSecondErr)
	})
}

func (n *NewClientFactoryTestSuite) TestGet() {
	n.Run("should return nil and error if provider name is empty", func() {
		registeredProviderName := "test_provider"
		newClientFactory := &exd.NewClientFactory{}
		newClient := func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return nil, nil
		}
		if err := newClientFactory.Add(registeredProviderName, newClient); err != nil {
			panic(err)
		}

		var testProviderName string

		actualNewClient, actualErr := newClientFactory.Get(testProviderName)

		n.Nil(actualNewClient)
		n.Error(actualErr)
	})

	n.Run("should return nil and error if provider name is not registered", func() {
		newClientFactory := &exd.NewClientFactory{}

		testProviderName := "test_provider"

		actualNewClient, actualErr := newClientFactory.Get(testProviderName)

		n.Nil(actualNewClient)
		n.Error(actualErr)
	})

	n.Run("should return client initializer and nil if no error is encountered", func() {
		registeredProviderName := "test_provider"
		newClientFactory := &exd.NewClientFactory{}
		newClient := func(ctx context.Context, httpDoer exd.HTTPDoer) (exd.Client, error) {
			return nil, nil
		}
		if err := newClientFactory.Add(registeredProviderName, newClient); err != nil {
			panic(err)
		}

		testProviderName := "test_provider"

		actualNewClient, actualErr := newClientFactory.Get(testProviderName)

		n.NotNil(actualNewClient)
		n.NoError(actualErr)
	})
}

func TestNewClientFactory(t *testing.T) {
	suite.Run(t, &NewClientFactoryTestSuite{})
}
