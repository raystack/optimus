package specio_test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/goto/optimus/client/local/model"
	"github.com/goto/optimus/client/local/specio"
)

type JobSpecReadWriterTestSuite struct {
	suite.Suite
}

func TestJobSpecReadWriter(t *testing.T) {
	s := new(JobSpecReadWriterTestSuite)
	suite.Run(t, s)
}

func (j *JobSpecReadWriterTestSuite) TestReadAll() {
	j.Run("return nil and error if root dir path is empty", func() {
		specFS := afero.NewMemMapFs()
		jobSpecReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		var rootDirPath string

		jobSpecs, err := jobSpecReadWriter.ReadAll(rootDirPath)

		j.Assert().Error(err)
		j.Assert().Nil(jobSpecs)
	})

	j.Run("return nil and error when discovering file paths is error", func() {
		specFS := afero.NewMemMapFs()
		jobSpecReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		rootDirPath := "invalid"

		jobSpecs, err := jobSpecReadWriter.ReadAll(rootDirPath)

		j.Assert().Error(err)
		j.Assert().Nil(jobSpecs)
	})

	j.Run("return nil and error when read one job spec is error", func() {
		specFS := afero.NewMemMapFs()
		err := j.writeTo(specFS, "root/ns1/jobs/example1/job.yaml", "invalid yaml")
		j.Require().NoError(err)

		jobSpecReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		rootDirPath := "root"

		jobSpecs, err := jobSpecReadWriter.ReadAll(rootDirPath)

		j.Assert().Error(err)
		j.Assert().ErrorContains(err, "yaml: unmarshal errors")
		j.Assert().Nil(jobSpecs)
	})

	j.Run("return job specs and nil when read all success", func() {
		specFS := j.createValidSpecFS(
			"root/ns1/jobs/example1",
			"root/ns1/jobs/example2",
		)
		err := j.writeTo(specFS, "root/ns1/this.yaml", `task:
  config:
    EXAMPLE: parent`)
		j.Require().NoError(err)

		jobSpecReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		rootDirPath := "root"

		jobSpecs, err := jobSpecReadWriter.ReadAll(rootDirPath)

		j.Assert().NoError(err)
		j.Assert().Len(jobSpecs, 2)
	})

	j.Run("return job specs containing parent config and nil if parent spec this.yaml is specified", func() {
		specFS := j.createValidSpecFS("root/parent/ns1/jobs/example1")
		err := j.writeTo(specFS, "root/parent/ns1/this.yaml", `task:
  config:
    EXAMPLE: parent_overwrite`)
		j.Require().NoError(err)
		err = j.writeTo(specFS, "root/parent/this.yaml", `task:
  config:
    EXAMPLE: parent
    EXAMPLE2: parent_no_overwrite`)
		j.Require().NoError(err)

		jobSpecReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		rootDirPath := "root"

		jobSpecs, err := jobSpecReadWriter.ReadAll(rootDirPath)

		j.Assert().NoError(err)
		j.Assert().Len(jobSpecs, 1)
		expectedTaskConfig := map[string]string{
			"PROJECT":     "godata",
			"DATASET":     "example",
			"TABLE":       "example1",
			"SQL_TYPE":    "STANDARD",
			"LOAD_METHOD": "REPLACE",
			"EXAMPLE":     "parent_overwrite",
			"EXAMPLE2":    "parent_no_overwrite",
		}
		j.Assert().Equal(expectedTaskConfig, jobSpecs[0].Task.Config)
	})

	j.Run("return job specs and nil if there is job which does not have asset", func() {
		specFS := j.createValidSpecFS("root/ns1/jobs/example1")
		err := specFS.RemoveAll("root/ns1/jobs/example1/assets")
		j.Require().NoError(err)

		jobSpecReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		rootDirPath := "root"

		jobSpecs, err := jobSpecReadWriter.ReadAll(rootDirPath)

		j.Assert().NoError(err)
		j.Assert().Len(jobSpecs, 1)
	})
}

func (j *JobSpecReadWriterTestSuite) TestReadByName() {
	j.Run("should return nil and error if root dir is empty", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		var rootDirPath string
		name := "example2"

		actualJobSpec, actualError := specReadWriter.ReadByName(rootDirPath, name)

		j.Assert().Nil(actualJobSpec)
		j.Assert().Error(actualError)
	})

	j.Run("should return nil and error if name is empty", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		rootDirPath := "namespace"
		var name string

		actualJobSpec, actualError := specReadWriter.ReadByName(rootDirPath, name)

		j.Assert().Nil(actualJobSpec)
		j.Assert().Error(actualError)
	})

	j.Run("should return nil and error if error is encountered when reading specs", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		rootDirPath := "namespace"
		name := "example2"

		actualJobSpec, actualError := specReadWriter.ReadByName(rootDirPath, name)

		j.Assert().Nil(actualJobSpec)
		j.Assert().Error(actualError)
	})

	j.Run("should return nil and error if spec with the specified name is not found", func() {
		specFS := j.createValidSpecFS("root/ns1/jobs/example1")
		specReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		rootDirPath := "root"
		name := "example2"

		actualJobSpec, actualError := specReadWriter.ReadByName(rootDirPath, name)

		j.Assert().Nil(actualJobSpec)
		j.Assert().Error(actualError)
	})

	j.Run("should return spec and nil if spec with the specified name is found", func() {
		specFS := j.createValidSpecFS("root/ns1/jobs/example1")
		specReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		rootDirPath := "root"
		name := "example1"

		actualJobSpec, actualError := specReadWriter.ReadByName(rootDirPath, name)

		j.Assert().EqualValues(name, actualJobSpec.Name)
		j.Assert().NoError(actualError)
	})
}

func (j *JobSpecReadWriterTestSuite) TestWrite() {
	j.Run("return error if file path is empty", func() {
		specFS := afero.NewMemMapFs()
		jobSpecReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		var filePath string
		jobSpec := &model.JobSpec{}

		err := jobSpecReadWriter.Write(filePath, jobSpec)

		j.Assert().Error(err)
	})

	j.Run("return error if job spec is nil", func() {
		specFS := afero.NewMemMapFs()
		jobSpecReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		filePath := "root/ns1/jobs/example1"
		var jobSpec *model.JobSpec

		err := jobSpecReadWriter.Write(filePath, jobSpec)

		j.Assert().Error(err)
	})

	j.Run("return error if job file path is restricted to write", func() {
		specFS := afero.NewMemMapFs()
		err := specFS.MkdirAll("root/ns1/jobs", os.ModeDir)
		j.Require().NoError(err)
		readOnlySpecFS := afero.NewReadOnlyFs(specFS)
		jobSpecReadWriter := specio.NewTestJobSpecReadWriter(readOnlySpecFS)

		filePath := "root/ns1/jobs/example1"
		jobSpec := &model.JobSpec{Version: 1}

		err = jobSpecReadWriter.Write(filePath, jobSpec)

		j.Assert().Error(err)
	})

	j.Run("return error when cannot create asset file", func() {
		specFS := afero.NewMemMapFs()
		re, err := regexp.Compile(`root/ns1/jobs/example1/job\.yaml`) // only allow to create job.yaml
		j.Require().NoError(err)
		specFS = afero.NewRegexpFs(specFS, re)
		filePath := "root/ns1/jobs/example1"
		jobSpec := model.JobSpec{Version: 1}
		jobSpec.Asset = map[string]string{
			"query.sql": "SELECT * FROM example",
		}

		jobSpecReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		err = jobSpecReadWriter.Write(filePath, &jobSpec)
		j.Assert().Error(err)
	})

	j.Run("return nil when success to create job spec and its assets", func() {
		specFS := afero.NewMemMapFs()
		filePath := "root/ns1/jobs/example1"
		jobSpec := model.JobSpec{Version: 1}
		jobSpec.Asset = map[string]string{
			"query.sql": "SELECT * FROM example",
		}

		jobSpecReadWriter := specio.NewTestJobSpecReadWriter(specFS)

		err := jobSpecReadWriter.Write(filePath, &jobSpec)
		j.Assert().NoError(err)
		jobYamlContent, err := j.readFrom(specFS, filepath.Join(filePath, "job.yaml"))
		j.Require().NoError(err)
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
hooks: []
dependencies: []
`
		j.Assert().Equal(expectedYamlContent, jobYamlContent)
		assetQueryContent, err := j.readFrom(specFS, filepath.Join(filePath, "assets", "query.sql"))
		j.Require().NoError(err)
		expectedAssetQueryContent := "SELECT * FROM example"
		j.Assert().Equal(expectedAssetQueryContent, assetQueryContent)
	})
}

func (j *JobSpecReadWriterTestSuite) createValidSpecFS(specDirPaths ...string) afero.Fs {
	templateJobSpec := `version: 1
name: %s
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

		j.writeTo(specFS, jobSpecFilePath, dataRaw)
		j.writeTo(specFS, assetFilePath, assetRaw)
	}

	return specFS
}

func (*JobSpecReadWriterTestSuite) writeTo(fs afero.Fs, filePath, content string) error {
	f, err := fs.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(content)

	return err
}

func (*JobSpecReadWriterTestSuite) readFrom(fs afero.Fs, filePath string) (string, error) {
	f, err := fs.Open(filePath)
	if err != nil {
		return "", err
	}

	content, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func TestNewJobSpecReadWriter(t *testing.T) {
	t.Run("return nil and error if spec fs is nil", func(t *testing.T) {
		var specFS afero.Fs

		jobSpecReadWriter, err := specio.NewJobSpecReadWriter(specFS)

		assert.Error(t, err)
		assert.Nil(t, jobSpecReadWriter)
	})

	t.Run("accept options and return job spec read writer and nil if no eror is encountered", func(t *testing.T) {
		specFS := afero.NewMemMapFs()

		jobSpecReadWriter, err := specio.NewJobSpecReadWriter(specFS, specio.WithJobSpecParentReading())

		assert.NoError(t, err)
		assert.NotNil(t, jobSpecReadWriter)
	})

	t.Run("return job spec read writer and nil if no error is encountered", func(t *testing.T) {
		specFS := afero.NewMemMapFs()

		jobSpecReadWriter, err := specio.NewJobSpecReadWriter(specFS)

		assert.NoError(t, err)
		assert.NotNil(t, jobSpecReadWriter)
	})
}
