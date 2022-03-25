package config_test

import (
	"io/fs"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/odpf/optimus/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
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

// (LEGACY)
const (
	configFileName       = ".optimus.yaml"
	optimusConfigDirName = "./optimus"
)

// (LEGACY)
const optimusConfigContent = `
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
namespaces:
- name: namespace-a
  job:
    path: ./jobs-a
- name: namespace-b
  job:
    path: ./jobs-b
`

// (LEGACY)
func setup(content string) {
	teardown()
	if err := os.Mkdir(optimusConfigDirName, 0o750); err != nil {
		panic(err)
	}
	confPath := path.Join(optimusConfigDirName, configFileName)
	if err := os.WriteFile(confPath, []byte(content), 0o660); err != nil {
		panic(err)
	}
}

func teardown() {
	if err := os.RemoveAll(optimusConfigDirName); err != nil {
		panic(err)
	}
}

// (LEGACY)
func TestLoadOptimusConfig(t *testing.T) {
	t.Run("should return config and nil if no error is found", func(t *testing.T) {
		setup(optimusConfigContent + `- name: namespace-b
  job:
    path: ./jobs-b`)
		defer teardown()

		expectedErrMsg := "namespaces [namespace-b] are duplicate"

		actualConf, actualErr := config.LoadOptimusConfig(optimusConfigDirName)

		assert.Nil(t, actualConf)
		assert.EqualError(t, actualErr, expectedErrMsg)
	})

	t.Run("should return config and nil if no error is found", func(t *testing.T) {
		setup(optimusConfigContent)
		defer teardown()

		expectedConf := &config.Optimus{
			Version: 1,
			Log: config.LogConfig{
				Level: "info",
			},
			Host: "localhost:9100",
			Project: config.Project{
				Name: "sample_project",
				Config: map[string]string{
					"environment":    "integration",
					"scheduler_host": "http://example.io/",
					"storage_path":   "file://absolute_path_to_a_directory",
				},
			},
			Server: config.Serve{
				Port:                9100,
				Host:                "localhost",
				IngressHost:         "optimus.example.io:80",
				AppKey:              "Yjo4a0jn1NvYdq79SADC/KaVv9Wu0Ffc",
				ReplayNumWorkers:    1,
				ReplayWorkerTimeout: 100 * time.Second,
				ReplayRunTimeout:    10 * time.Second,
				DB: config.DBConfig{
					DSN:               "postgres://user:password@localhost:5432/database?sslmode=disable",
					MaxIdleConnection: 5,
					MaxOpenConnection: 10,
				},
			},
			Scheduler: config.SchedulerConfig{
				Name:     "airflow2",
				SkipInit: true,
			},
			Telemetry: config.TelemetryConfig{
				ProfileAddr: ":9110",
				JaegerAddr:  "http://localhost:14268/api/traces",
			},
			Namespaces: []*config.Namespace{
				{
					Name: "namespace-a",
					Job: config.Job{
						Path: "./jobs-a",
					},
				},
				{
					Name: "namespace-b",
					Job: config.Job{
						Path: "./jobs-b",
					},
				},
			},
		}

		actualConf, actualErr := config.LoadOptimusConfig(optimusConfigDirName)

		assert.EqualValues(t, expectedConf, actualConf)
		assert.NoError(t, actualErr)
	})
}

type ConfigTestSuite struct {
	suite.Suite
	a        afero.Afero
	currPath string
	execPath string
	homePath string

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
	s.execPath = p
	s.a.Fs.MkdirAll(s.execPath, fs.ModeTemporary)

	p, err = os.UserHomeDir()
	s.Require().NoError(err)
	s.homePath = p
	s.a.Fs.MkdirAll(s.homePath, fs.ModeTemporary)

	config.FS = s.a.Fs

	s.initExpectedClientConfig()
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

func (s *ConfigTestSuite) TestLoadClientConfig() {
	currFilePath := path.Join(s.currPath, config.DefaultFilename+"."+config.DefaultFileExtension)
	s.a.WriteFile(currFilePath, []byte(projectConfig), fs.ModeTemporary)

	s.Run("WhenFilepathIsEmpty", func() {
		s.Run("WhenConfigInCurrentPathIsExist", func() {
			conf, err := config.LoadClientConfig(config.EmptyPath)

			s.Assert().NoError(err)
			s.Assert().NotNil(conf)
			s.Assert().Equal(s.expectedClientConfig, conf)
		})

		s.Run("WhenConfigInCurrentPathNotExist", func() {
			s.a.Remove(currFilePath)
			defer s.a.WriteFile(currFilePath, []byte(projectConfig), fs.ModeTemporary)

			conf, err := config.LoadClientConfig(config.EmptyPath)
			s.Assert().NoError(err)
			s.Assert().NotNil(conf)
		})
	})

	s.Run("WhenFilepathIsExist", func() {
		s.Run("WhenFilePathIsvalid", func() {
			samplePath := "./sample/path/config.yaml"
			b := strings.Builder{}
			b.WriteString(projectConfig)
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

			s.Assert().Error(err)
			s.Assert().Nil(conf)
		})
	})
}

func (s *ConfigTestSuite) TestLoadServerConfig() {
	execFilePath := path.Join(s.execPath, config.DefaultFilename+"."+config.DefaultFileExtension)
	homeFilePath := path.Join(s.homePath, config.DefaultFilename+"."+config.DefaultFileExtension)
	s.a.WriteFile(execFilePath, []byte(serverConfig), fs.ModeTemporary)
	s.a.WriteFile(homeFilePath, []byte(`version: 3`), fs.ModeTemporary)
	s.initServerConfigEnv()

	s.Run("WhenFilepathIsEmpty", func() {
		s.Run("WhenEnvExist", func() {
			conf, err := config.LoadServerConfig(config.EmptyPath)

			s.Assert().NoError(err)
			s.Assert().NotNil(conf)
			s.Assert().Equal("4", conf.Version.String()) // should load from env var
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

			s.Assert().NoError(err)
			s.Assert().NotNil(conf)
			s.Assert().Equal("3", conf.Version.String()) // should load from home dir
		})

		s.Run("WhenConfigNotFound", func() {
			s.a.Remove(execFilePath)
			s.a.Remove(homeFilePath)
			defer s.a.WriteFile(execFilePath, []byte(serverConfig), fs.ModeTemporary)
			defer s.a.WriteFile(homeFilePath, []byte(`version: 3`), fs.ModeTemporary)

			conf, err := config.LoadServerConfig(config.EmptyPath)
			s.Assert().NoError(err)
			s.Assert().NotNil(conf)
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
		})

		s.Run("WhenFilePathIsNotValid", func() {
			s.a.MkdirAll("/path/dir/", os.ModeTemporary)
			conf, err := config.LoadServerConfig("/path/dir/")
			s.Assert().Error(err)
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
	s.expectedServerConfig.Serve.ReplayNumWorkers = 1
	s.expectedServerConfig.Serve.ReplayWorkerTimeout = 100 * time.Second
	s.expectedServerConfig.Serve.ReplayRunTimeout = 10 * time.Second
	s.expectedServerConfig.Serve.DB = config.DBConfig{}
	s.expectedServerConfig.Serve.DB.DSN = "postgres://user:password@localhost:5432/database?sslmode=disable"
	s.expectedServerConfig.Serve.DB.MaxIdleConnection = 5
	s.expectedServerConfig.Serve.DB.MaxOpenConnection = 10

	s.expectedServerConfig.Scheduler = config.SchedulerConfig{}
	s.expectedServerConfig.Scheduler.Name = "airflow2"
	s.expectedServerConfig.Scheduler.SkipInit = true

	s.expectedServerConfig.Telemetry = config.TelemetryConfig{}
	s.expectedServerConfig.Telemetry.ProfileAddr = ":9110"
	s.expectedServerConfig.Telemetry.JaegerAddr = "http://localhost:14268/api/traces"
}

func (s *ConfigTestSuite) initServerConfigEnv() {
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

func (s *ConfigTestSuite) unsetServerConfigEnv() {
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
