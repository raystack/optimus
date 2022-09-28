package local_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/odpf/optimus/client/local"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gopkg.in/yaml.v2"
)

type JobSpecReadWriterTestSuite struct {
	suite.Suite
}

func TestJobSpecReadWriter(t *testing.T) {
	s := new(JobSpecReadWriterTestSuite)
	suite.Run(t, s)
}

func TestNewJobSpecReadWriter(t *testing.T) {
	t.Run("when specFS is nil", func(t *testing.T) {
		jobSpecReadWriter, err := local.NewJobSpecReadWriter(nil)

		assert.Error(t, err)
		assert.Nil(t, jobSpecReadWriter)
	})

	t.Run("when specFS is valid", func(t *testing.T) {
		specFS := afero.NewMemMapFs()

		jobSpecReadWriter, err := local.NewJobSpecReadWriter(specFS)

		assert.NoError(t, err)
		assert.NotNil(t, jobSpecReadWriter)
	})
}

func (s *JobSpecReadWriterTestSuite) TestReadAll() {
	s.Run("return nil and error when discovering file paths is error", func() {
		specFS := afero.NewMemMapFs()
		jobSpecReadWriter, err := local.NewJobSpecReadWriter(specFS)
		s.Require().NoError(err)

		invalidRootPath := "invalid"
		jobSpecs, err := jobSpecReadWriter.ReadAll(invalidRootPath)

		s.Assert().Error(err)
		s.Assert().Nil(jobSpecs)
	})

	s.Run("return nil and error when read one job spec is error", func() {
		specFS := afero.NewMemMapFs()
		err := writeTo(specFS, "root/ns1/jobs/example1/job.yaml", "invalid yaml")
		s.Require().NoError(err)

		jobSpecReadWriter, err := local.NewJobSpecReadWriter(specFS)
		s.Require().NoError(err)

		jobSpecs, err := jobSpecReadWriter.ReadAll("root")
		s.Assert().Error(err)
		s.Assert().ErrorContains(err, "yaml: unmarshal errors")
		s.Assert().Nil(jobSpecs)
	})

	s.Run("return jobSpec and nil when read all success", func() {
		specFS := createValidSpecFS(
			"root/ns1/jobs/example1",
			"root/ns1/jobs/example2",
		)
		err := writeTo(specFS, "root/ns1/this.yaml", `task:
  config:
    EXAMPLE: parent`)
		s.Require().NoError(err)

		jobReaderWriter, err := local.NewJobSpecReadWriter(specFS)
		s.Require().NoError(err)

		jobSpecs, err := jobReaderWriter.ReadAll("root")
		s.Assert().NoError(err)
		s.Assert().Len(jobSpecs, 2)
	})

	s.Run("return jobSpec and nil when contains parent spec this.yaml", func() {
		specFS := createValidSpecFS("root/parent/ns1/jobs/example1")
		err := writeTo(specFS, "root/parent/ns1/this.yaml", `task:
  config:
    EXAMPLE: parent_overwrite`)
		s.Require().NoError(err)
		err = writeTo(specFS, "root/parent/this.yaml", `task:
  config:
    EXAMPLE: parent
    EXAMPLE2: parent_no_overwrite`)
		s.Require().NoError(err)

		jobReaderWriter, err := local.NewJobSpecReadWriter(specFS)
		s.Require().NoError(err)

		jobSpecs, err := jobReaderWriter.ReadAll("root")
		s.Assert().NoError(err)
		s.Assert().Len(jobSpecs, 1)
		expectedTaskConfig := []yaml.MapItem{
			{Key: "PROJECT", Value: "godata"},
			{Key: "DATASET", Value: "example"},
			{Key: "TABLE", Value: "example1"},
			{Key: "SQL_TYPE", Value: "STANDARD"},
			{Key: "LOAD_METHOD", Value: "REPLACE"},
			{Key: "EXAMPLE", Value: "parent_overwrite"},
			{Key: "EXAMPLE2", Value: "parent_no_overwrite"},
		}
		s.Assert().Equal(yaml.MapSlice(expectedTaskConfig), jobSpecs[0].Task.Config)
	})
}

func createValidSpecFS(specDirPaths ...string) afero.Fs {
	templateJobSpec := `version: 1
name: godata.ds.%s
owner: optimus@optimus.dev
schedule:
  start_date: "2022-03-22"
  interval: 0 22 * * * 
behavior:
  depends_on_past: true
  catch_up: false
  notify:
    - on: test
      channel:
        - test://hello
task:
  name: bq2bq
  config:
    PROJECT: godata
    DATASET: example
    TABLE: %s
    SQL_TYPE: STANDARD
    LOAD_METHOD: REPLACE
  window:
    size: 24h
    offset: 24h
    truncate_to: d
labels:
  orchestrator: optimus
dependencies: 
  - 
    http: 
      name: http-sensor-1
      headers: 
        Authentication: Token-1
        Content-type: application/json
      params: 
        key-1: value-1
        key-2: value-2
      url: "https://optimus-host:80/serve/1/"
  - 
    http: 
      name: http-sensor-2
      headers: 
        Authentication: Token-2
        Content-type: application/json
      params: 
        key-3: value-3
        key-4: value-4
      url: "https://optimus-host:80/serve/2/"
hooks: []`
	templateAsset := `SELECT * FROM %s`

	specFS := afero.NewMemMapFs()

	for _, specDirPath := range specDirPaths {
		splittedDirPath := strings.Split(specDirPath, "/")
		jobName := splittedDirPath[len(splittedDirPath)-1]

		dataRaw := fmt.Sprintf(templateJobSpec, jobName, jobName)
		assetRaw := fmt.Sprintf(templateAsset, jobName)

		jobSpecFilePath := filepath.Join(specDirPath, "job.yaml")
		assetFilePath := filepath.Join(specDirPath, "assets", "query.sql")

		writeTo(specFS, jobSpecFilePath, dataRaw)
		writeTo(specFS, assetFilePath, assetRaw)
	}

	return specFS
}

func writeTo(fs afero.Fs, filePath string, content string) error {
	f, err := fs.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(content)

	return err
}
