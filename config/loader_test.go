package config

import (
	"io/fs"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/suite"
)

const projectConfig = `
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
const serverConfig = `
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

	s.initExpectedProjectConfig()
	s.initExpectedServerConfig()
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
	s.a.WriteFile(path.Join(s.execPath, filename+"."+fileExtension), []byte(serverConfig), fs.ModeTemporary)
	s.a.WriteFile(path.Join(s.homePath, filename+"."+fileExtension), []byte(`version: 3`), fs.ModeTemporary)

	s.Run("WhenFilepathIsEmpty", func() {
		conf, err := loadServerConfigFs(s.a.Fs)
		s.Assert().NoError(err)
		s.Assert().NotNil(conf)
		s.Assert().Equal(s.expectedServerConfig, conf) // should load from exec path

		s.a.Remove(path.Join(s.execPath, filename+"."+fileExtension))
		defer s.a.WriteFile(path.Join(s.execPath, filename+"."+fileExtension), []byte(serverConfig), fs.ModeTemporary)
		conf, err = loadServerConfigFs(s.a.Fs)
		s.Assert().NoError(err)
		s.Assert().NotNil(conf)
		s.Assert().Equal("3", conf.Version.String()) // should load from home dir
	})

	s.Run("WhenFilepathIsExist", func() {
		samplePath := "./sample/path/config.yaml"
		s.a.WriteFile(samplePath, []byte("version: 2"), os.ModeTemporary)

		conf, err := loadServerConfigFs(s.a.Fs, samplePath)
		s.Assert().NoError(err)
		s.Assert().NotNil(conf)
		s.Assert().Equal("2", conf.Version.String())
	})

	s.Run("WhenLoadConfigIsFailed", func() {
		dirPath := "./sample/path"
		s.a.Fs.MkdirAll(dirPath, os.ModeTemporary)

		conf, err := loadServerConfigFs(s.a.Fs, dirPath)
		s.Assert().Error(err)
		s.Assert().Nil(conf)
	})
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

func (s *ConfigTestSuite) initExpectedProjectConfig() {
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

func (s *ConfigTestSuite) initExpectedServerConfig() {
	s.expectedServerConfig = &ServerConfig{}
	s.expectedServerConfig.Version = Version(1)
	s.expectedServerConfig.Log = LogConfig{Level: "info"}

	s.expectedServerConfig.Serve = Serve{}
	s.expectedServerConfig.Serve.Port = 9100
	s.expectedServerConfig.Serve.Host = "localhost"
	s.expectedServerConfig.Serve.IngressHost = "optimus.example.io:80"
	s.expectedServerConfig.Serve.AppKey = "Yjo4a0jn1NvYdq79SADC/KaVv9Wu0Ffc"
	s.expectedServerConfig.Serve.ReplayNumWorkers = 1
	s.expectedServerConfig.Serve.ReplayWorkerTimeout = 100 * time.Second
	s.expectedServerConfig.Serve.ReplayRunTimeout = 10 * time.Second
	s.expectedServerConfig.Serve.DB = DBConfig{}
	s.expectedServerConfig.Serve.DB.DSN = "postgres://user:password@localhost:5432/database?sslmode=disable"
	s.expectedServerConfig.Serve.DB.MaxIdleConnection = 5
	s.expectedServerConfig.Serve.DB.MaxOpenConnection = 10

	s.expectedServerConfig.Scheduler = SchedulerConfig{}
	s.expectedServerConfig.Scheduler.Name = "airflow2"
	s.expectedServerConfig.Scheduler.SkipInit = true

	s.expectedServerConfig.Telemetry = TelemetryConfig{}
	s.expectedServerConfig.Telemetry.ProfileAddr = ":9110"
	s.expectedServerConfig.Telemetry.JaegerAddr = "http://localhost:14268/api/traces"
}
