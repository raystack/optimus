package config_test

import (
	"testing"

	"github.com/odpf/optimus/config"
	"github.com/stretchr/testify/suite"
)

type ValidationTestSuite struct {
	suite.Suite
	defaultClientConfig *config.ClientConfig
}

func (s *ValidationTestSuite) SetupTest() {
	s.initDefaultClientConfig()
}

func TestValidation(t *testing.T) {
	suite.Run(t, new(ValidationTestSuite))
}

func (s *ValidationTestSuite) TestValidate() {
	s.Run("WhenValidateClientConfig", func() {
		s.Run("WhenConfigIsValid", func() {
			err := config.Validate(s.defaultClientConfig)
			s.Assert().NoError(err)
		})

		s.Run("WhenNamespacesIsDuplicated", func() {
			clientConfig := s.defaultClientConfig
			namespaces := clientConfig.Namespaces
			namespaces = append(namespaces, &config.Namespace{Name: "ns-dup"})
			namespaces = append(namespaces, &config.Namespace{Name: "ns-dup"})
			clientConfig.Namespaces = namespaces

			err := config.Validate(clientConfig)

			s.Assert().Error(err)
		})
	})

	s.Run("WhenValidateServerConfig", func() {
		// TODO: implement this
		s.T().Skip()
	})

	s.Run("WhenValidateTypeIsInvalid", func() {
		err := config.Validate("invalid-type")
		s.Assert().Error(err)
	})
}

func (s *ValidationTestSuite) initDefaultClientConfig() {
	s.defaultClientConfig = &config.ClientConfig{}
	s.defaultClientConfig.Version = config.Version(1)
	s.defaultClientConfig.Log = config.LogConfig{Level: config.LogLevelInfo}

	s.defaultClientConfig.Host = "localhost:9100"
	s.defaultClientConfig.Project = config.Project{
		Name: "sample_project",
		Config: map[string]string{
			"environment":    "integration",
			"scheduler_host": "http://example.io/",
			"storage_path":   "file://absolute_path_to_a_directory",
		},
	}
	namespaces := []*config.Namespace{}
	namespaces = append(namespaces, &config.Namespace{
		Name: "namespace-a",
		Job: config.Job{
			Path: "./jobs-a",
		},
	})
	namespaces = append(namespaces, &config.Namespace{
		Name: "namespace-b",
		Job: config.Job{
			Path: "./jobs-b",
		},
	})
	namespaces = append(namespaces, nil)
	s.defaultClientConfig.Namespaces = namespaces
}
