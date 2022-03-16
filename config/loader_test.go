package config_test

import (
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/config"
)

const (
	configFileName       = ".optimus.yaml"
	optimusConfigDirName = "./optimus"
)

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

func TestLoadOptimusConfig(t *testing.T) {
	t.Run("should return config and nil if no error is found", func(t *testing.T) {
		setup(optimusConfigContent + `
- name: namespace-b
  job:
    path: ./jobs-b
`)
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
