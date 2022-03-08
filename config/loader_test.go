package config_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/odpf/optimus/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

type ConfigType int

const (
	server ConfigType = iota
	project
	namespace
)

const (
	rawServer string = `
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
	rawProject string = `
version: 1
host: localhost:9100
project:
  name: sample_project
  config:
    environment: integration
    scheduler_host: http://example.io/
    storage_path: file://absolute_path_to_a_directory
`
	rawNamespace string = `
version: 1
namespace:
  name: sample_namespace
  job:
    path: "./job"
  datastore:
    type: bigquery
    path: "./bq"
  config: {}
`
)

func createConfig(fs afero.Fs, filePath string, confType ConfigType) error {
	switch confType {
	case server:
		return afero.WriteFile(fs, filePath, []byte(rawServer), 0644)
	case project:
		return afero.WriteFile(fs, filePath, []byte(rawProject), 0644)
	case namespace:
		return afero.WriteFile(fs, filePath, []byte(rawNamespace), 0644)
	}
	return errors.New("")
}

func TestLoadConfig(t *testing.T) {
	testCases := []struct {
		name       string
		path       string
		configType ConfigType
		err        error
	}{
		{
			name:       "LoadFromRoot_NoError",
			path:       "/",
			configType: server,
			err:        nil,
		},
		{
			name:       "LoadFromRootProject_NoError",
			path:       "/home/projects/project1",
			configType: server,
			err:        nil,
		},
		{
			name:       "LoadFromExecutionFolder_NoError",
			path:       "/usr/bin",
			configType: server,
			err:        nil,
		},
		{
			name:       "LoadAsProjectStructure_NoError",
			path:       "/",
			configType: project,
			err:        nil,
		},
	}
	fs := afero.NewMemMapFs()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := createConfig(fs, filepath.Join(tc.path, ".optimus.yaml"), tc.configType)
			assert.NoError(t, err)

			optimus := config.Optimus{}
			err = config.LoadConfig(&optimus, fs, tc.path)
			assert.NoError(t, err)
			// TODO: check the value
		})
	}
}

func TestLoadNamespaceConfig(t *testing.T) {
	t.Run("NoNamespacesDetected", func(t *testing.T) {
		fs := afero.NewMemMapFs()
		path := "/project1"
		fs.MkdirAll(path, 0755)

		namespaces := map[string]*config.Namespace{}
		err := config.LoadNamespacesConfig(namespaces, fs, path)
		assert.NoError(t, err)
		assert.Len(t, namespaces, 0)
		// TODO: check the value
	})

	t.Run("WithFolderWithNamespaces", func(t *testing.T) {
		fs := afero.NewMemMapFs()
		path := "/project2"
		fs.MkdirAll(filepath.Join(path, "ns1"), 0755)

		err := createConfig(fs, filepath.Join("/project2/ns1", ".optimus.yaml"), namespace)
		assert.NoError(t, err)

		namespaces := map[string]*config.Namespace{}
		err = config.LoadNamespacesConfig(namespaces, fs, path)
		assert.NoError(t, err)
		assert.Len(t, namespaces, 1)
		// TODO: check the value
	})
}
