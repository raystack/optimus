package config_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/config"
)

type ClientConfigTestSuite struct {
	suite.Suite
}

func (c *ClientConfigTestSuite) TestGetNamespaceByName() {
	c.Run("should return nil and error if name is not found", func() {
		namespaceName := "namespace1"
		clientConfig := &config.ClientConfig{}

		actualNamespace, actualErr := clientConfig.GetNamespaceByName(namespaceName)

		c.Nil(actualNamespace)
		c.Error(actualErr)
	})

	c.Run("should namespace and nil if namespace name is found", func() {
		namespaceName := "namespace1"
		clientConfig := &config.ClientConfig{
			Namespaces: []*config.Namespace{
				{
					Name: namespaceName,
				},
			},
		}

		actualNamespace, actualErr := clientConfig.GetNamespaceByName(namespaceName)

		c.NotNil(actualNamespace)
		c.NoError(actualErr)
	})
}

func (c *ClientConfigTestSuite) TestValidateNamespaceNames() {
	c.Run("should return error if one or more namespaces are not found", func() {
		namespaceName := "namespace1"
		clientConfig := &config.ClientConfig{}

		actualErr := clientConfig.ValidateNamespaceNames(namespaceName)

		c.Error(actualErr)
	})

	c.Run("should return nil if all namespaces are found", func() {
		namespaceName := "namespace1"
		clientConfig := &config.ClientConfig{
			Namespaces: []*config.Namespace{
				{
					Name: namespaceName,
				},
			},
		}

		actualErr := clientConfig.ValidateNamespaceNames(namespaceName)

		c.NoError(actualErr)
	})
}

func (c *ClientConfigTestSuite) TestGetSelectedNamespaces() {
	c.Run("should return nil and error if one or more namespaces are not found", func() {
		namespaceName := "namespace1"
		clientConfig := &config.ClientConfig{}

		actualNamespaces, actualErr := clientConfig.GetSelectedNamespaces(namespaceName)

		c.Nil(actualNamespaces)
		c.Error(actualErr)
	})

	c.Run("should return namespaces and nil if all namespaces are found", func() {
		namespaceName := "namespace1"
		clientConfig := &config.ClientConfig{
			Namespaces: []*config.Namespace{
				{
					Name: namespaceName,
				},
			},
		}

		actualNamespaces, actualErr := clientConfig.GetSelectedNamespaces(namespaceName)

		c.NotNil(actualNamespaces)
		c.NoError(actualErr)
	})
}

func (c *ClientConfigTestSuite) TestGetAllNamespaceNames() {
	c.Run("should return all namespace names", func() {
		namespaceName := "namespace1"
		clientConfig := &config.ClientConfig{
			Namespaces: []*config.Namespace{
				{
					Name: namespaceName,
				},
			},
		}

		actualNamespaceNames := clientConfig.GetAllNamespaceNames()

		c.NotNil(actualNamespaceNames)
	})
}

func TestClientConfigSuite(t *testing.T) {
	suite.Run(t, new(ClientConfigTestSuite))
}
