package config

import (
	"io/fs"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/suite"
)

const (
	projectConfig = `
version: 1
log:
  level: info
host: "localhost:9100"
project:
  name: sample_project
  config:
    environment: integration
    scheduler_host: http://example.io/
    storage_path: file://absolute_path_to_a_directory
namespaces:
- name: namespace-a
  job:
    path: ./jobs-a
- name: namespace-b
  job:
    path: ./jobs-b
`
	serverConfig = `
version: 1
log:
  level: info
serve:
  port: 9100
  host: localhost
  ingress_host: optimus.example.io:80
  app_key: Yjo4a0jn1NvYdq79SADC/KaVv9Wu0Ffc
  replay_num_workers: 1
  replay_worker_timeout: "100s"
  replay_run_timeout: "10s"
  db:
    dsn: postgres://user:password@localhost:5432/database?sslmode=disable
    max_idle_connection: 5
    max_open_connection: 10
scheduler:
  name: airflow2
  skip_init: true
telemetry:
  profile_addr: ":9110"
  jaeger_addr: "http://localhost:14268/api/traces"
`
)

type ConfigTestSuite struct {
	suite.Suite
	a        afero.Afero
	currPath string
	execPath string
	homePath string

	expectedProjectConfig *ProjectConfig
	expectedServerConfig  *ServerConfig
}

func (s *ConfigTestSuite) SetupTest() {
	s.a = afero.Afero{}
	s.a.Fs = afero.NewMemMapFs()

	p, err := os.Getwd()
	s.Require().NoError(err)
	s.currPath = p
	s.a.Fs.MkdirAll(s.currPath, fs.ModeTemporary)

	p, err = os.Executable()
	s.Require().NoError(err)
	s.execPath = p
	s.a.Fs.MkdirAll(s.execPath, fs.ModeTemporary)

	p, err = os.UserHomeDir()
	s.Require().NoError(err)
	s.homePath = p
	s.a.Fs.MkdirAll(s.homePath, fs.ModeTemporary)

	s.expectedProjectConfig = &ProjectConfig{}
	s.expectedProjectConfig.Version = Version(1)
	s.expectedProjectConfig.Log = LogConfig{Level: "info"}

	s.expectedProjectConfig.Host = "localhost:9100"
	s.expectedProjectConfig.Project = Project{
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
	s.expectedProjectConfig.Namespaces = namespaces
}

func (s *ConfigTestSuite) TearDownTest() {
	s.a.Fs.RemoveAll(s.currPath)
	s.a.Fs.RemoveAll(s.execPath)
	s.a.Fs.RemoveAll(s.homePath)
}

func TestConfig(t *testing.T) {
	suite.Run(t, new(ConfigTestSuite))
}

func (s *ConfigTestSuite) TestInternal_LoadProjectConfigFs() {
	s.a.WriteFile(path.Join(s.currPath, filename+"."+fileExtension), []byte(projectConfig), fs.ModeTemporary)

	s.Run("WhenFilepathIsEmpty", func() {
		p, err := loadProjectConfigFs(s.a.Fs)
		s.Assert().NoError(err)
		s.Assert().NotNil(p)
		s.Assert().Equal(s.expectedProjectConfig, p)
	})

	s.Run("WhenFilepathIsExist", func() {
		samplePath := "./sample/path/config.yaml"
		b := strings.Builder{}
		b.WriteString(projectConfig)
		b.WriteString(`- name: namespace-c
  job:
    path: ./jobs-c
    `)
		s.a.WriteFile(samplePath, []byte(b.String()), fs.ModeTemporary)
		defer s.a.Fs.RemoveAll(samplePath)

		p, err := loadProjectConfigFs(s.a.Fs, samplePath)
		s.Assert().NoError(err)
		s.Assert().NotNil(p)
		s.Assert().Len(p.Namespaces, 3)
	})

	s.Run("WhenLoadConfigIsFailed", func() {
		p, err := loadProjectConfigFs(s.a.Fs, "/path/not/exist")
		s.Assert().Error(err)
		s.Assert().Nil(p)
	})
}

func (s *ConfigTestSuite) TestInternal_LoadServerConfigFs() {
	// TODO: implement this
}

func (s *ConfigTestSuite) TestLoadProjectConfig() {
	// TODO: implement this
}

func (s *ConfigTestSuite) TestMustLoadProjectConfig() {
	// TODO: implement this
}

func (s *ConfigTestSuite) TestLoadServerConfig() {
	// TODO: implement this
}

func (s *ConfigTestSuite) TestMustLoadServerConfig() {
	// TODO: implement this
}

// func setup(content string) {
// 	teardown()
// 	if err := os.Mkdir(optimusConfigDirName, os.ModePerm); err != nil {
// 		panic(err)
// 	}
// 	confPath := path.Join(optimusConfigDirName, configFileName)
// 	if err := os.WriteFile(confPath, []byte(content), os.ModePerm); err != nil {
// 		panic(err)
// 	}
// }

// func teardown() {
// 	if err := os.RemoveAll(optimusConfigDirName); err != nil {
// 		panic(err)
// 	}
// }

// func TestLoadOptimusConfig(t *testing.T) {
// 	t.Run("should return config and nil if no error is found", func(t *testing.T) {
// 		setup(optimusConfigContent + `
// - name: namespace-b
//   job:
//     path: ./jobs-b
// `)
// 		defer teardown()

// 		expectedErrMsg := "namespaces [namespace-b] are duplicate"

// 		actualConf, actualErr := config.LoadOptimusConfig(optimusConfigDirName)

// 		assert.Nil(t, actualConf)
// 		assert.EqualError(t, actualErr, expectedErrMsg)
// 	})

// 	t.Run("should return config and nil if no error is found", func(t *testing.T) {
// 		setup(optimusConfigContent)
// 		defer teardown()

// 		expectedConf := &config.Optimus{
// 			Version: 1,
// 			Log: config.LogConfig{
// 				Level: "info",
// 			},
// 			Host: "localhost:9100",
// 			Project: config.Project{
// 				Name: "sample_project",
// 				Config: map[string]string{
// 					"environment":    "integration",
// 					"scheduler_host": "http://example.io/",
// 					"storage_path":   "file://absolute_path_to_a_directory",
// 				},
// 			},
// 			Server: config.ServerConfig{
// 				Port:                9100,
// 				Host:                "localhost",
// 				IngressHost:         "optimus.example.io:80",
// 				AppKey:              "Yjo4a0jn1NvYdq79SADC/KaVv9Wu0Ffc",
// 				ReplayNumWorkers:    1,
// 				ReplayWorkerTimeout: 100 * time.Second,
// 				ReplayRunTimeout:    10 * time.Second,
// 				DB: config.DBConfig{
// 					DSN:               "postgres://user:password@localhost:5432/database?sslmode=disable",
// 					MaxIdleConnection: 5,
// 					MaxOpenConnection: 10,
// 				},
// 			},
// 			Scheduler: config.SchedulerConfig{
// 				Name:     "airflow2",
// 				SkipInit: true,
// 			},
// 			Telemetry: config.TelemetryConfig{
// 				ProfileAddr: ":9110",
// 				JaegerAddr:  "http://localhost:14268/api/traces",
// 			},
// 			Namespaces: []*config.Namespace{
// 				{
// 					Name: "namespace-a",
// 					Job: config.Job{
// 						Path: "./jobs-a",
// 					},
// 				},
// 				{
// 					Name: "namespace-b",
// 					Job: config.Job{
// 						Path: "./jobs-b",
// 					},
// 				},
// 			},
// 		}

// 		actualConf, actualErr := config.LoadOptimusConfig(optimusConfigDirName)

// 		assert.EqualValues(t, expectedConf, actualConf)
// 		assert.NoError(t, actualErr)
// 	})
// }
