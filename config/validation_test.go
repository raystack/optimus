package config

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type ValidationTestSuite struct {
	suite.Suite
	defaultProjectConfig ClientConfig
}

func (s *ValidationTestSuite) SetupTest() {
	s.initDefaultProjectConfig()
}

func TestValidation(t *testing.T) {
	suite.Run(t, new(ValidationTestSuite))
}

func (s *ValidationTestSuite) TestInternal_ValidateNamespaces_Success() {
	namespaces := []*Namespace{
		{
			Name: "namespace-1",
			Job:  Job{Path: "path-1"},
		}, {
			Name: "namespace-2",
			Job:  Job{Path: "path-2"},
		},
		nil,
	}

	err := validateNamespaces(namespaces)
	s.Assert().NoError(err)
}

func (s *ValidationTestSuite) TestInternal_ValidateNamespaces_Fail() {
	s.Run("WhenTypeAssertionIsFailed", func() {
		invalidStruct := "this-is-string"
		err := validateNamespaces(invalidStruct)
		s.Assert().Error(err)
	})

	s.Run("WhenDuplicationIsDetected", func() {
		namespaces := []*Namespace{
			{
				Name: "other-ns",
				Job:  Job{Path: "path-other"},
			}, {
				Name: "dup-ns",
				Job:  Job{Path: "path-1"},
			}, {
				Name: "dup-ns",
				Job:  Job{Path: "path-2"},
			},
		}

		err := validateNamespaces(namespaces)
		s.Assert().Error(err)
	})
}

func (s *ValidationTestSuite) TestValidate_ProjectConfig() {
	s.Run("WhenStructIsValid", func() {
		err := Validate(s.defaultProjectConfig)
		s.Assert().NoError(err)
	})

	s.Run("WhenStructIsInvalid", func() {
		conf := s.defaultProjectConfig
		conf.Host = ""

		err := Validate(conf)
		s.Assert().Error(err)
	})
}

func (s *ValidationTestSuite) TestValidate_ServerConfig() {
	// TODO: implement this
}

func (s *ValidationTestSuite) TestValidate_Fail() {
	err := Validate("invalid-type")
	s.Assert().Error(err)
}

func (s *ValidationTestSuite) initDefaultProjectConfig() {
	s.defaultProjectConfig = ClientConfig{}
	s.defaultProjectConfig.Version = Version(1)
	s.defaultProjectConfig.Log = LogConfig{Level: "info"}

	s.defaultProjectConfig.Host = "localhost:9100"
	s.defaultProjectConfig.Project = Project{
		Name: "sample_project",
		Config: map[string]string{
			"environment":    "integration",
			"scheduler_host": "http://example.io/",
			"storage_path":   "file://absolute_path_to_a_directory",
		},
	}
	namespaces := []*Namespace{}
	namespaces = append(namespaces, &Namespace{
		Name: "namespace-a",
		Job: Job{
			Path: "./jobs-a",
		},
	})
	namespaces = append(namespaces, &Namespace{
		Name: "namespace-b",
		Job: Job{
			Path: "./jobs-b",
		},
	})
	s.defaultProjectConfig.Namespaces = namespaces
}
