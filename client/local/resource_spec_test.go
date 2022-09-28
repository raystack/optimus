package local_test

import (
	"io"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/odpf/optimus/client/local"
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
		specReadWriter := local.NewTestResourceSpecReadWriter(specFS)

		var rootDirPath string

		actualResourceSpecs, actualError := specReadWriter.ReadAll(rootDirPath)

		r.Assert().Nil(actualResourceSpecs)
		r.Assert().Error(actualError)
	})

	r.Run("should return nil and error if encountered error when discovering spec dir paths", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := local.NewTestResourceSpecReadWriter(specFS)

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
		specReadWriter := local.NewTestResourceSpecReadWriter(specFS)

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
		specReadWriter := local.NewTestResourceSpecReadWriter(specFS)

		expectedResourceSpecs := []*local.ResourceSpec{
			{
				Version: 1,
				Name:    "project.dataset.user",
				Type:    "table",
				Labels: map[string]string{
					"orchestrator": "optimus",
				},
				Spec: []interface{}{
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

		actualResourceSpecs, actualError := specReadWriter.ReadAll(rootDirPath)

		r.Assert().EqualValues(expectedResourceSpecs, actualResourceSpecs)
		r.Assert().NoError(actualError)
	})
}

func (r *ResourceSpecReadWriterTestSuite) TestWrite() {
	r.Run("should return error if dir path is empty", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := local.NewTestResourceSpecReadWriter(specFS)

		var dirPath string
		spec := &local.ResourceSpec{}

		actualError := specReadWriter.Write(dirPath, spec)

		r.Assert().Error(actualError)
	})

	r.Run("should return error if spec is nil", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := local.NewTestResourceSpecReadWriter(specFS)

		dirPath := "namespace"
		var spec *local.ResourceSpec

		actualError := specReadWriter.Write(dirPath, spec)

		r.Assert().Error(actualError)
	})

	r.Run("should write spec to file and return nil if no error is encountered", func() {
		specFS := afero.NewMemMapFs()
		specReadWriter := local.NewTestResourceSpecReadWriter(specFS)

		dirPath := "namespace"
		spec := &local.ResourceSpec{
			Version: 1,
			Name:    "project.dataset.user",
			Type:    "table",
			Labels: map[string]string{
				"orchestrator": "optimus",
			},
			Spec: []interface{}{
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
spec:
    - mode: nullable
      name: id
      type: string
    - mode: nullable
      name: name
      type: string
labels:
    orchestrator: optimus
`)
		r.Assert().EqualValues(expectedContent, actualContent)
	})
}

func TestNewResourceSpecReadWriter(t *testing.T) {
	t.Run("should return nil and error if spec fs is nil", func(t *testing.T) {
		var specFS afero.Fs

		actualResourceSpecReadWriter, actualError := local.NewResourceSpecReadWriter(specFS)

		assert.Nil(t, actualResourceSpecReadWriter)
		assert.Error(t, actualError)
	})

	t.Run("should job resource spec read writer and nil if spec fs is not nil", func(t *testing.T) {
		specFS := afero.NewMemMapFs()

		actualResourceSpecReadWriter, actualError := local.NewResourceSpecReadWriter(specFS)

		assert.NotNil(t, actualResourceSpecReadWriter)
		assert.NoError(t, actualError)
	})
}
