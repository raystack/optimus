package local_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/odpf/optimus/client/local"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
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
		expectedTaskConfig := map[string]string{
			"PROJECT":     "godata",
			"DATASET":     "example",
			"TABLE":       "example1",
			"SQL_TYPE":    "STANDARD",
			"LOAD_METHOD": "REPLACE",
			"EXAMPLE":     "parent_overwrite",
			"EXAMPLE2":    "parent_no_overwrite",
		}
		s.Assert().Equal(expectedTaskConfig, jobSpecs[0].Task.Config)
	})
}

func (s *JobSpecReadWriterTestSuite) TestWrite() {
	s.Run("return error when job spec is nil", func() {
		specFS := afero.NewMemMapFs()
		filePath := "root/ns1/jobs/example1"

		jobSpecReadWriter, err := local.NewJobSpecReadWriter(specFS)
		s.Require().NoError(err)

		err = jobSpecReadWriter.Write(filePath, nil)
		s.Assert().Error(err)
	})

	s.Run("return error when job file path is restricted to write", func() {
		specFS := afero.NewMemMapFs()
		err := specFS.MkdirAll("root/ns1/jobs", os.ModeDir)
		s.Require().NoError(err)
		specFS = afero.NewReadOnlyFs(specFS)
		filePath := "root/ns1/jobs/example1"
		jobSpec := local.JobSpec{Version: 1}

		jobSpecReadWriter, err := local.NewJobSpecReadWriter(specFS)
		s.Require().NoError(err)

		err = jobSpecReadWriter.Write(filePath, &jobSpec)
		s.Assert().Error(err)
	})

	s.Run("return error when coudln't create asset file", func() {
		specFS := afero.NewMemMapFs()
		re, err := regexp.Compile(`root/ns1/jobs/example1/job\.yaml`) // only allow to create job.yaml
		s.Require().NoError(err)
		specFS = afero.NewRegexpFs(specFS, re)
		filePath := "root/ns1/jobs/example1"
		jobSpec := local.JobSpec{Version: 1}
		jobSpec.Asset = map[string]string{
			"query.sql": "SELECT * FROM example",
		}

		jobSpecReadWriter, err := local.NewJobSpecReadWriter(specFS)
		s.Require().NoError(err)

		err = jobSpecReadWriter.Write(filePath, &jobSpec)
		s.Assert().Error(err)
	})

	s.Run("return nil when success to create job spec and its assets", func() {
		specFS := afero.NewMemMapFs()
		filePath := "root/ns1/jobs/example1"
		jobSpec := local.JobSpec{Version: 1}
		jobSpec.Asset = map[string]string{
			"query.sql": "SELECT * FROM example",
		}

		jobSpecReadWriter, err := local.NewJobSpecReadWriter(specFS)
		s.Require().NoError(err)

		err = jobSpecReadWriter.Write(filePath, &jobSpec)
		s.Assert().NoError(err)
		jobYamlContent, err := readFrom(specFS, filepath.Join(filePath, "job.yaml"))
		s.Require().NoError(err)
		expectedYamlContent := `version: 1
name: ""
owner: ""
schedule:
    start_date: ""
    interval: ""
behavior:
    depends_on_past: false
    catch_up: false
task:
    name: ""
    window:
        size: ""
        offset: ""
        truncate_to: ""
dependencies: []
hooks: []
`
		s.Assert().Equal(expectedYamlContent, jobYamlContent)
		assetQueryContent, err := readFrom(specFS, filepath.Join(filePath, "assets", "query.sql"))
		s.Require().NoError(err)
		expectedAssetQueryContent := "SELECT * FROM example"
		s.Assert().Equal(expectedAssetQueryContent, assetQueryContent)
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

func readFrom(fs afero.Fs, filePath string) (string, error) {
	f, err := fs.Open(filePath)
	if err != nil {
		return "", err
	}

	content, err := ioutil.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(content), nil
}
