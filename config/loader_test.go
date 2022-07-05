package config_test

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	saltConfig "github.com/odpf/salt/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/config"
)

const clientConfig = `
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
  replay:
    num_workers: 1
    worker_timeout: "100s"
    run_timeout: "10s"
  deployer:
    num_workers: 1
    worker_timeout: "100s"
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

	expectedClientConfig *config.ClientConfig
	expectedServerConfig *config.ServerConfig
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
	s.execPath = filepath.Dir(p)
	s.a.Fs.MkdirAll(s.execPath, fs.ModeTemporary)

	config.FS = s.a.Fs

	s.initExpectedClientConfig()
	s.initExpectedServerConfig()
}

func (s *ConfigTestSuite) TearDownTest() {
	s.a.Fs.RemoveAll(s.currPath)
	s.a.Fs.RemoveAll(s.execPath)
}

func TestConfig(t *testing.T) {
	suite.Run(t, new(ConfigTestSuite))
}

func (s *ConfigTestSuite) TestLoadClientConfig() {
	currFilePath := path.Join(s.currPath, config.DefaultFilename)
	s.a.WriteFile(currFilePath, []byte(clientConfig), fs.ModeTemporary)

	s.Run("WhenFilepathIsEmpty", func() {
		s.Run("WhenConfigInCurrentPathIsExist", func() {
			conf, err := config.LoadClientConfig(config.EmptyPath)

			s.Assert().NoError(err)
			s.Assert().NotNil(conf)
			s.Assert().Equal(s.expectedClientConfig, conf)
		})

		s.Run("WhenConfigInCurrentPathNotExist", func() {
			s.a.Remove(currFilePath)
			defer s.a.WriteFile(currFilePath, []byte(clientConfig), fs.ModeTemporary)

			conf, err := config.LoadClientConfig(config.EmptyPath)
			s.Assert().NotNil(err)
			s.Assert().ErrorAs(err, &saltConfig.ConfigFileNotFoundError{})
			s.Assert().Nil(conf)
		})
	})

	s.Run("WhenFilepathIsExist", func() {
		s.Run("WhenFilePathIsvalid", func() {
			samplePath := "./sample/path/config.yaml"
			b := strings.Builder{}
			b.WriteString(clientConfig)
			b.WriteString(`- name: namespace-c
  job:
    path: ./jobs-c
    `)
			s.a.WriteFile(samplePath, []byte(b.String()), fs.ModeTemporary)
			defer s.a.Fs.RemoveAll(samplePath)

			conf, err := config.LoadClientConfig(samplePath)

			s.Assert().NoError(err)
			s.Assert().NotNil(conf)
			s.Assert().Len(conf.Namespaces, 3)
		})

		s.Run("WhenFilePathIsNotValid", func() {
			conf, err := config.LoadClientConfig("/path/not/exist")

			s.Assert().NotNil(err)
			s.Assert().Nil(conf)
		})
	})
}

func (s *ConfigTestSuite) TestLoadServerConfig() {
	execFilePath := path.Join(s.execPath, config.DefaultConfigFilename)
	s.a.WriteFile(execFilePath, []byte(serverConfig), fs.ModeTemporary)
	s.initServerConfigEnv()

	s.Run("WhenFilepathIsEmpty", func() {
		s.Run("WhenEnvExist", func() {
			conf, err := config.LoadServerConfig(config.EmptyPath)

			s.Assert().NoError(err)
			s.Assert().NotNil(conf)
			s.Assert().Equal("4", conf.Version.String()) // should load from env var
			s.Assert().Equal("INFO", conf.Log.Level.String())
		})

		s.Run("WhenEnvNotExist", func() {
			s.unsetServerConfigEnv()
			defer s.initServerConfigEnv()

			conf, err := config.LoadServerConfig(config.EmptyPath)

			s.Assert().NoError(err)
			s.Assert().NotNil(conf)
			s.Assert().EqualValues(s.expectedServerConfig, conf) // should load from exec dir
		})

		s.Run("WhenEnvNotExistAndExecDirNotExist", func() {
			s.unsetServerConfigEnv()
			s.a.Remove(execFilePath)
			defer s.initServerConfigEnv()
			defer s.a.WriteFile(execFilePath, []byte(serverConfig), fs.ModeTemporary)

			conf, err := config.LoadServerConfig(config.EmptyPath)
			s.Assert().NotNil(err)
			s.Assert().ErrorAs(err, &saltConfig.ConfigFileNotFoundError{})
			s.Assert().Nil(conf)
		})
	})

	s.Run("WhenFilepathIsExist", func() {
		s.Run("WhenFilePathIsValid", func() {
			samplePath := "./sample/path/config.yaml"
			s.a.WriteFile(samplePath, []byte("version: 2"), os.ModeTemporary)

			conf, err := config.LoadServerConfig(samplePath)

			s.Assert().NoError(err)
			s.Assert().NotNil(conf)
			s.Assert().Equal("2", conf.Version.String())
			s.Assert().Equal("INFO", conf.Log.Level.String())
		})

		s.Run("WhenFilePathIsNotValid", func() {
			s.a.MkdirAll("/path/dir/", os.ModeTemporary)
			conf, err := config.LoadServerConfig("/path/dir/")
			s.Assert().NotNil(err)
			s.Assert().Nil(conf)
		})
	})
}

func (s *ConfigTestSuite) initExpectedClientConfig() {
	s.expectedClientConfig = &config.ClientConfig{}
	s.expectedClientConfig.Version = config.Version(1)
	s.expectedClientConfig.Log = config.LogConfig{Level: config.LogLevelInfo}

	s.expectedClientConfig.Host = "localhost:9100"
	s.expectedClientConfig.Project = config.Project{
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
	s.expectedClientConfig.Namespaces = namespaces
}

func (s *ConfigTestSuite) initExpectedServerConfig() {
	s.expectedServerConfig = &config.ServerConfig{}
	s.expectedServerConfig.Version = config.Version(1)
	s.expectedServerConfig.Log = config.LogConfig{Level: config.LogLevelInfo}

	s.expectedServerConfig.Serve = config.Serve{}
	s.expectedServerConfig.Serve.Port = 9100
	s.expectedServerConfig.Serve.Host = "localhost"
	s.expectedServerConfig.Serve.IngressHost = "optimus.example.io:80"
	s.expectedServerConfig.Serve.AppKey = "Yjo4a0jn1NvYdq79SADC/KaVv9Wu0Ffc"
	s.expectedServerConfig.Serve.Replay.NumWorkers = 1
	s.expectedServerConfig.Serve.Replay.WorkerTimeout = 100 * time.Second
	s.expectedServerConfig.Serve.Replay.RunTimeout = 10 * time.Second
	s.expectedServerConfig.Serve.Deployer.NumWorkers = 1
	s.expectedServerConfig.Serve.Deployer.WorkerTimeout = 100 * time.Second
	s.expectedServerConfig.Serve.Deployer.QueueCapacity = 10
	s.expectedServerConfig.Serve.DB = config.DBConfig{}
	s.expectedServerConfig.Serve.DB.DSN = "postgres://user:password@localhost:5432/database?sslmode=disable"
	s.expectedServerConfig.Serve.DB.MaxIdleConnection = 5
	s.expectedServerConfig.Serve.DB.MaxOpenConnection = 10

	s.expectedServerConfig.Scheduler = config.SchedulerConfig{}
	s.expectedServerConfig.Scheduler.Name = "airflow2"

	s.expectedServerConfig.Telemetry = config.TelemetryConfig{}
	s.expectedServerConfig.Telemetry.ProfileAddr = ":9110"
	s.expectedServerConfig.Telemetry.JaegerAddr = "http://localhost:14268/api/traces"
}

func (*ConfigTestSuite) initServerConfigEnv() {
	os.Setenv("OPTIMUS_VERSION", "4")
	os.Setenv("OPTIMUS_LOG_LEVEL", "info")
	os.Setenv("OPTIMUS_SERVE_PORT", "9100")
	os.Setenv("OPTIMUS_SERVE_HOST", "localhost")
	os.Setenv("OPTIMUS_SERVE_INGRESS_HOST", "optimus.example.io:80")
	os.Setenv("OPTIMUS_SERVE_APP_KEY", "Yjo4a0jn1NvYdq79SADC/KaVv9Wu0Ffc")
	os.Setenv("OPTIMUS_SERVE_REPLAY_NUM_WORKERS", "1")
	os.Setenv("OPTIMUS_SERVE_REPLAY_WORKER_TIMEOUT", "100s")
	os.Setenv("OPTIMUS_SERVE_REPLAY_RUN_TIMEOUT", "10s")
	os.Setenv("OPTIMUS_SERVE_DB_DSN", "postgres://user:password@localhost:5432/database?sslmode=disable")
	os.Setenv("OPTIMUS_SERVE_DB_MAX_IDLE_CONNECTION", "5")
	os.Setenv("OPTIMUS_SERVE_DB_MAX_OPEN_CONNECTION", "10")
	os.Setenv("OPTIMUS_SCHEDULER_NAME", "airflow2")
	os.Setenv("OPTIMUS_SCHEDULER_SKIP_INIT", "true")
	os.Setenv("OPTIMUS_TELEMETRY_PROFILE_ADDR", ":9110")
	os.Setenv("OPTIMUS_TELEMETRY_JAEGER_ADDR", "http://localhost:14268/api/traces")
}

func (*ConfigTestSuite) unsetServerConfigEnv() {
	unsetServerConfigEnv()
}

func unsetServerConfigEnv() {
	os.Unsetenv("OPTIMUS_VERSION")
	os.Unsetenv("OPTIMUS_LOG_LEVEL")
	os.Unsetenv("OPTIMUS_SERVE_PORT")
	os.Unsetenv("OPTIMUS_SERVE_HOST")
	os.Unsetenv("OPTIMUS_SERVE_INGRESS_HOST")
	os.Unsetenv("OPTIMUS_SERVE_APP_KEY")
	os.Unsetenv("OPTIMUS_SERVE_REPLAY_NUM_WORKERS")
	os.Unsetenv("OPTIMUS_SERVE_REPLAY_WORKER_TIMEOUT")
	os.Unsetenv("OPTIMUS_SERVE_REPLAY_RUN_TIMEOUT")
	os.Unsetenv("OPTIMUS_SERVE_DB_DSN")
	os.Unsetenv("OPTIMUS_SERVE_DB_MAX_IDLE_CONNECTION")
	os.Unsetenv("OPTIMUS_SERVE_DB_MAX_OPEN_CONNECTION")
	os.Unsetenv("OPTIMUS_SCHEDULER_NAME")
	os.Unsetenv("OPTIMUS_SCHEDULER_SKIP_INIT")
	os.Unsetenv("OPTIMUS_TELEMETRY_PROFILE_ADDR")
	os.Unsetenv("OPTIMUS_TELEMETRY_JAEGER_ADDR")
}
