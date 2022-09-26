package local_test

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/odpf/optimus/client/local"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type JobReadWriterTestSuite struct {
	suite.Suite
}

func TestJobReaderWriter(t *testing.T) {
	s := new(JobReadWriterTestSuite)
	suite.Run(t, s)
}

func TestNewJobReadWriter(t *testing.T) {
	t.Run("when specFS is nil", func(t *testing.T) {
		jobSpecReadWriter, err := local.NewJobSpecReadWriter(nil)

		assert.Error(t, err)
		assert.Nil(t, jobSpecReadWriter)
	})

	t.Run("when specFS is valid", func(t *testing.T) {
		specFS := fstest.MapFS{}

		jobSpecReadWriter, err := local.NewJobSpecReadWriter(specFS)

		assert.NoError(t, err)
		assert.NotNil(t, jobSpecReadWriter)
	})
}

func (s *JobReadWriterTestSuite) TestReadAll() {
	s.Run("return nil and error when discovering file paths is error", func() {
		specFS := fstest.MapFS{}
		jobSpecReadWriter, err := local.NewJobSpecReadWriter(specFS)
		s.Require().NoError(err)

		invalidRootPath := "invalid"
		jobSpecs, err := jobSpecReadWriter.ReadAll(invalidRootPath)
		s.Assert().Error(err)
		s.Assert().Nil(jobSpecs)
	})
	s.Run("return nil and error when read one job spec is error", func() {
		specFS := fstest.MapFS{
			"root/ns1/jobs/example1/job.yaml": {
				Data: []byte(`invalid yaml`),
			},
		}
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
			"root/ns2/jobs/example3",
			"root/ns2/jobs/example4",
		)
		jobReaderWriter, err := local.NewJobSpecReadWriter(specFS)
		s.Require().NoError(err)

		jobSpecs, err := jobReaderWriter.ReadAll("root")
		s.Assert().NoError(err)
		s.Assert().Len(jobSpecs, 4)
	})

}

func createValidSpecFS(specDirPaths ...string) fs.FS {
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

	specFS := fstest.MapFS{}

	for _, specDirPath := range specDirPaths {
		splittedDirPath := strings.Split(specDirPath, "/")
		jobName := splittedDirPath[len(splittedDirPath)-1]

		dataRaw := fmt.Sprintf(templateJobSpec, jobName, jobName)
		assetRaw := fmt.Sprintf(templateAsset, jobName)

		jobSpecFilePath := filepath.Join(specDirPath, "job.yaml")
		assetFilePath := filepath.Join(specDirPath, "assets", "query.sql")

		specFS[jobSpecFilePath] = &fstest.MapFile{
			Data: []byte(dataRaw),
		}
		specFS[assetFilePath] = &fstest.MapFile{
			Data: []byte(assetRaw),
		}
	}

	return specFS
}
