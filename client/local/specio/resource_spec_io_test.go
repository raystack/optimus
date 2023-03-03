package specio_test

import (
	"io"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/goto/optimus/client/local/model"
	"github.com/goto/optimus/client/local/specio"
)

type ResourceSpecReadWriterTestSuite struct {
	suite.Suite
}

func TestResourceSpecReadWriter(t *testing.T) {
	suite.Run(t, &ResourceSpecReadWriterTestSuite{})
}

func (r *ResourceSpecReadWriterTestSuite) TestReadAll() {
	r.Run("should return nil and error if root dir path is empty", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := specio.NewTestResourceSpecReadWriter(specFS)

		var rootDirPath string

		actualResourceSpecs, actualError := specReadWriter.ReadAll(rootDirPath)

		r.Assert().Nil(actualResourceSpecs)
		r.Assert().Error(actualError)
	})

	r.Run("should return nil and error if encountered error when discovering spec dir paths", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := specio.NewTestResourceSpecReadWriter(specFS)

		rootDirPath := "invalid_dir_path"

		actualResourceSpecs, actualError := specReadWriter.ReadAll(rootDirPath)

		r.Assert().Nil(actualResourceSpecs)
		r.Assert().Error(actualError)
	})

	r.Run("should return nil and error if encountered error when unmarshalling spec", func() {
		specFS := afero.NewMemMapFs()
		fileSpec, _ := specFS.Create("namespace/resource/user/resource.yaml")
		fileSpec.WriteString("invalid yaml")
		fileSpec.Close()
		specReadWriter := specio.NewTestResourceSpecReadWriter(specFS)

		rootDirPath := "namespace"

		actualResourceSpecs, actualError := specReadWriter.ReadAll(rootDirPath)

		r.Assert().Nil(actualResourceSpecs)
		r.Assert().Error(actualError)
	})

	r.Run("should return specs and nil if no error encountered", func() {
		rawSpecContent := `
  version: 1
  name: project.dataset.user
  type: table
  labels:
    orchestrator: optimus
  spec:
    schema:
    - name: id
      type: string
      mode: nullable
    - name: name
      type: string
      mode: nullable
`
		specFS := afero.NewMemMapFs()
		fileSpec, _ := specFS.Create("namespace/resource/user/resource.yaml")
		fileSpec.WriteString(rawSpecContent)
		fileSpec.Close()
		specReadWriter := specio.NewTestResourceSpecReadWriter(specFS)

		expectedResourceSpecs := []*model.ResourceSpec{
			{
				Version: 1,
				Name:    "project.dataset.user",
				Type:    "table",
				Labels: map[string]string{
					"orchestrator": "optimus",
				},
				Spec: map[string]interface{}{
					"schema": []interface{}{
						map[string]interface{}{
							"name": "id",
							"type": "string",
							"mode": "nullable",
						},
						map[string]interface{}{
							"name": "name",
							"type": "string",
							"mode": "nullable",
						},
					},
				},
			},
		}

		rootDirPath := "namespace"

		actualResourceSpecs, actualError := specReadWriter.ReadAll(rootDirPath)

		r.Assert().EqualValues(expectedResourceSpecs, actualResourceSpecs)
		r.Assert().NoError(actualError)
	})
}

func (r *ResourceSpecReadWriterTestSuite) TestReadByName() {
	r.Run("should return nil and error if root dir is empty", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := specio.NewTestResourceSpecReadWriter(specFS)

		var rootDirPath string
		name := "resource"

		actualResourceSpec, actualError := specReadWriter.ReadByName(rootDirPath, name)

		r.Assert().Nil(actualResourceSpec)
		r.Assert().Error(actualError)
	})

	r.Run("should return nil and error if name is empty", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := specio.NewTestResourceSpecReadWriter(specFS)

		rootDirPath := "namespace"
		var name string

		actualResourceSpec, actualError := specReadWriter.ReadByName(rootDirPath, name)

		r.Assert().Nil(actualResourceSpec)
		r.Assert().Error(actualError)
	})

	r.Run("should return nil and error if error is encountered when reading specs", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := specio.NewTestResourceSpecReadWriter(specFS)

		rootDirPath := "namespace"
		name := "resource"

		actualResourceSpec, actualError := specReadWriter.ReadByName(rootDirPath, name)

		r.Assert().Nil(actualResourceSpec)
		r.Assert().Error(actualError)
	})

	r.Run("should return nil and error if spec with the specified name is not found", func() {
		rawSpecContent := `
  version: 1
  name: project.dataset.user
  type: table
  labels:
    orchestrator: optimus
  spec:
    schema:
    - name: id
      type: string
      mode: nullable
    - name: name
      type: string
      mode: nullable
`
		specFS := afero.NewMemMapFs()
		fileSpec, _ := specFS.Create("namespace/resource/user/resource.yaml")
		fileSpec.WriteString(rawSpecContent)
		fileSpec.Close()
		specReadWriter := specio.NewTestResourceSpecReadWriter(specFS)

		rootDirPath := "namespace"
		name := "resource"

		actualResourceSpec, actualError := specReadWriter.ReadByName(rootDirPath, name)

		r.Assert().Nil(actualResourceSpec)
		r.Assert().Error(actualError)
	})

	r.Run("should return spec and nil if spec with the specified name is found", func() {
		rawSpecContent := `
  version: 1
  name: project.dataset.user
  type: table
  labels:
    orchestrator: optimus
  spec:
    schema:
    - name: id
      type: string
      mode: nullable
    - name: name
      type: string
      mode: nullable
`
		specFS := afero.NewMemMapFs()
		fileSpec, _ := specFS.Create("namespace/resource/user/resource.yaml")
		fileSpec.WriteString(rawSpecContent)
		fileSpec.Close()
		specReadWriter := specio.NewTestResourceSpecReadWriter(specFS)

		expectedResourceSpecs := &model.ResourceSpec{
			Version: 1,
			Name:    "project.dataset.user",
			Type:    "table",
			Labels: map[string]string{
				"orchestrator": "optimus",
			},
			Spec: map[string]interface{}{
				"schema": []interface{}{
					map[string]interface{}{
						"name": "id",
						"type": "string",
						"mode": "nullable",
					},
					map[string]interface{}{
						"name": "name",
						"type": "string",
						"mode": "nullable",
					},
				},
			},
		}

		rootDirPath := "namespace"
		name := "project.dataset.user"

		actualResourceSpec, actualError := specReadWriter.ReadByName(rootDirPath, name)

		r.Assert().EqualValues(expectedResourceSpecs, actualResourceSpec)
		r.Assert().NoError(actualError)
	})
}

func (r *ResourceSpecReadWriterTestSuite) TestWrite() {
	r.Run("should return error if dir path is empty", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := specio.NewTestResourceSpecReadWriter(specFS)

		var dirPath string
		spec := &model.ResourceSpec{}

		actualError := specReadWriter.Write(dirPath, spec)

		r.Assert().Error(actualError)
	})

	r.Run("should return error if spec is nil", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := specio.NewTestResourceSpecReadWriter(specFS)

		dirPath := "namespace"
		var spec *model.ResourceSpec

		actualError := specReadWriter.Write(dirPath, spec)

		r.Assert().Error(actualError)
	})

	r.Run("should write spec to file and return nil if no error is encountered", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := specio.NewTestResourceSpecReadWriter(specFS)

		dirPath := "namespace"
		spec := &model.ResourceSpec{
			Version: 1,
			Name:    "project.dataset.user",
			Type:    "table",
			Labels: map[string]string{
				"orchestrator": "optimus",
			},
			Spec: map[string]interface{}{
				"schema": []map[string]interface{}{
					{
						"name": "id",
						"type": "string",
						"mode": "nullable",
					},
					{
						"name": "name",
						"type": "string",
						"mode": "nullable",
					},
				},
			},
		}

		actualError := specReadWriter.Write(dirPath, spec)
		r.Assert().NoError(actualError)

		specFile, err := specFS.Open("namespace/resource.yaml")
		r.Assert().NoError(err)

		actualContent, err := io.ReadAll(specFile)
		r.Assert().NoError(err)

		expectedContent := []byte(`version: 1
name: project.dataset.user
type: table
labels:
  orchestrator: optimus
spec:
  schema:
    - mode: nullable
      name: id
      type: string
    - mode: nullable
      name: name
      type: string
`)
		r.Assert().EqualValues(expectedContent, actualContent)
	})
}

func TestNewResourceSpecReadWriter(t *testing.T) {
	t.Run("should return nil and error if spec fs is nil", func(t *testing.T) {
		var specFS afero.Fs

		actualResourceSpecReadWriter, actualError := specio.NewResourceSpecReadWriter(specFS)

		assert.Nil(t, actualResourceSpecReadWriter)
		assert.Error(t, actualError)
	})

	t.Run("should job resource spec read writer and nil if spec fs is not nil", func(t *testing.T) {
		specFS := afero.NewMemMapFs()

		actualResourceSpecReadWriter, actualError := specio.NewResourceSpecReadWriter(specFS)

		assert.NotNil(t, actualResourceSpecReadWriter)
		assert.NoError(t, actualError)
	})
}
