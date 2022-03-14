package config_test

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/odpf/optimus/config"

	"github.com/stretchr/testify/assert"
)

const (
	configFileName          = ".optimus.yaml"
	optimusConfigDirName    = "./optimus"
	namespaceConfigADirName = "./namespace-a"
	namespaceConfigBDirName = "./namespace-b"
	optimusConfigContent    = `
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
`
	namespaceConfigAContent = `
version: 1
namespace:
  name: namespace-a
  job:
    path: ./jobs
`
	namespaceConfigBContent = `
version: 1
namespace:
  name: namespace-b
  job:
    path: ./jobs
`
)

func setup() {
	teardown()
	if err := os.Mkdir(optimusConfigDirName, os.ModePerm); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(path.Join(optimusConfigDirName, namespaceConfigADirName), os.ModePerm); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(path.Join(optimusConfigDirName, namespaceConfigBDirName), os.ModePerm); err != nil {
		panic(err)
	}
	confPath := path.Join(optimusConfigDirName, configFileName)
	if err := os.WriteFile(confPath, []byte(optimusConfigContent), os.ModePerm); err != nil {
		panic(err)
	}
	confPath = path.Join(optimusConfigDirName, namespaceConfigADirName, configFileName)
	if err := os.WriteFile(confPath, []byte(namespaceConfigAContent), os.ModePerm); err != nil {
		panic(err)
	}
	confPath = path.Join(optimusConfigDirName, namespaceConfigBDirName, configFileName)
	if err := os.WriteFile(confPath, []byte(namespaceConfigBContent), os.ModePerm); err != nil {
		panic(err)
	}
}

func teardown() {
	if err := os.RemoveAll(optimusConfigDirName); err != nil {
		panic(err)
	}
}

func TestLoadOptimusConfig(t *testing.T) {
	setup()
	defer teardown()

	t.Run("should return config and nil if no error is found", func(t *testing.T) {
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
			Server: config.ServerConfig{
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
		}

		actualConf, actualErr := config.LoadOptimusConfig(optimusConfigDirName)

		assert.EqualValues(t, expectedConf, actualConf)
		assert.NoError(t, actualErr)
	})
}

func TestLoadNamespaceConfig(t *testing.T) {
	setup()
	defer teardown()

	t.Run("should return config and nil if no error is found", func(t *testing.T) {
		expectedConf := map[string]*config.Namespace{
			"namespace-a": {
				Name: "namespace-a",
				Job: config.Job{
					Path: "optimus/namespace-a/jobs",
				},
			},
			"namespace-b": {
				Name: "namespace-b",
				Job: config.Job{
					Path: "optimus/namespace-b/jobs",
				},
			},
		}

		actualConf, actualErr := config.LoadNamespacesConfig("./optimus")

		assert.EqualValues(t, expectedConf, actualConf)
		assert.NoError(t, actualErr)
	})
}
