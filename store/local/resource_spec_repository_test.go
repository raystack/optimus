package local_test

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/spf13/afero"

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/mock"
)

var testResourceContents = `version: 1
name: proj.datas.test
type: table
spec:
  labels:
    one: valueone
    two: valuetwo
  description: some description
  schema:
  - name: col1
    type: integer
`

func TestResourceSpecRepository(t *testing.T) {
	// prepare mocked datastore
	dsTypeTableAdapter := new(mock.DatastoreTypeAdapter)

	dsTypeTableController := new(mock.DatastoreTypeController)
	dsTypeTableController.On("Adapter").Return(dsTypeTableAdapter)

	dsTypeDatasetController := new(mock.DatastoreTypeController)
	dsTypeDatasetController.On("Adapter").Return(dsTypeTableAdapter)

	dsController := map[models.ResourceType]models.DatastoreTypeController{
		models.ResourceTypeTable:   dsTypeTableController,
		models.ResourceTypeDataset: dsTypeDatasetController,
	}
	datastorer := new(mock.Datastorer)
	datastorer.On("Types").Return(dsController)

	specTable := models.ResourceSpec{
		Version:   1,
		Name:      "proj.datas.test",
		Type:      models.ResourceTypeTable,
		Datastore: datastorer,
		Spec:      nil,
		Assets: map[string]string{
			"query.sql": "select * from 1",
		},
	}
	specTableWithoutAssets := models.ResourceSpec{
		Version:   1,
		Name:      "proj.datas.test",
		Type:      models.ResourceTypeTable,
		Datastore: datastorer,
		Spec:      nil,
	}

	specInBytes := []byte(testResourceContents)
	dsTypeTableAdapter.On("ToYaml", specTable).Return(specInBytes, nil)
	dsTypeTableAdapter.On("FromYaml", specInBytes).Return(specTableWithoutAssets, nil)

	t.Run("Save", func(t *testing.T) {
		t.Run("should write the file to ${ROOT}/${name}.yaml", func(t *testing.T) {
			appFS := afero.NewMemMapFs()

			repo := local.NewResourceSpecRepository(appFS, datastorer)
			err := repo.Save(specTable)
			assert.Nil(t, err)

			buf, err := afero.ReadFile(appFS, filepath.Join(specTable.Name, local.ResourceSpecFileName))
			assert.Nil(t, err)
			assert.Equal(t, testResourceContents, string(buf))

			bufQ, err := afero.ReadFile(appFS, filepath.Join(specTable.Name, "query.sql"))
			assert.Nil(t, err)
			asset, _ := specTable.Assets.GetByName("query.sql")
			assert.Equal(t, asset, string(bufQ))
		})
		t.Run("should return error if type is empty", func(t *testing.T) {
			repo := local.NewResourceSpecRepository(nil, datastorer)
			err := repo.Save(models.ResourceSpec{Name: "foo"})
			assert.NotNil(t, err)
		})
		t.Run("should return error if name is empty", func(t *testing.T) {
			repo := local.NewResourceSpecRepository(nil, datastorer)
			err := repo.Save(models.ResourceSpec{})
			assert.NotNil(t, err)
		})
	})

	t.Run("GetByName", func(t *testing.T) {
		t.Run("should open the file ${ROOT}/${name}.yaml and parse its contents", func(t *testing.T) {
			// create test files and directories
			appFS := afero.NewMemMapFs()
			appFS.MkdirAll(specTable.Name, 0755)
			afero.WriteFile(appFS, filepath.Join(specTable.Name, local.ResourceSpecFileName), []byte(testResourceContents), 0644)
			afero.WriteFile(appFS, filepath.Join(specTable.Name, "query.sql"), []byte(specTable.Assets["query.sql"]), 0644)

			repo := local.NewResourceSpecRepository(appFS, datastorer)
			returnedSpec, err := repo.GetByName(specTable.Name)
			assert.Nil(t, err)
			assert.Equal(t, specTable, returnedSpec)
		})
		t.Run("should use cache if file is requested more than once", func(t *testing.T) {
			// create test files and directories
			appFS := afero.NewMemMapFs()
			appFS.MkdirAll(specTable.Name, 0755)
			afero.WriteFile(appFS, filepath.Join(specTable.Name, local.ResourceSpecFileName), []byte(testResourceContents), 0644)
			afero.WriteFile(appFS, filepath.Join(specTable.Name, "query.sql"), []byte(specTable.Assets["query.sql"]), 0644)

			repo := local.NewResourceSpecRepository(appFS, datastorer)
			returnedSpec, err := repo.GetByName(specTable.Name)
			assert.Nil(t, err)
			assert.Equal(t, specTable, returnedSpec)

			// delete all specs
			assert.Nil(t, appFS.RemoveAll(specTable.Name))

			returnedSpecAgain, err := repo.GetByName(specTable.Name)
			assert.Nil(t, err)
			assert.Equal(t, specTable, returnedSpecAgain)
		})
		t.Run("should return ErrNoResources in case no job folder exist", func(t *testing.T) {
			repo := local.NewResourceSpecRepository(afero.NewMemMapFs(), datastorer)
			_, err := repo.GetByName(specTable.Name)
			assert.Equal(t, models.ErrNoResources, err)
		})
		t.Run("should return ErrNoResources in case the folder exist but no resource file exist", func(t *testing.T) {
			appFS := afero.NewMemMapFs()
			appFS.MkdirAll(specTable.Name, 0755)

			repo := local.NewResourceSpecRepository(appFS, datastorer)
			_, err := repo.GetByName(specTable.Name)
			assert.Equal(t, models.ErrNoResources, err)
		})
		t.Run("should return an error if name is empty", func(t *testing.T) {
			repo := local.NewResourceSpecRepository(afero.NewMemMapFs(), nil)
			_, err := repo.GetByName("")
			assert.NotNil(t, err)
		})
		t.Run("should return error if yaml source is incorrect and failed to validate", func(t *testing.T) {
			// create test files and directories
			appFS := afero.NewMemMapFs()
			appFS.MkdirAll(specTable.Name, 0755)
			afero.WriteFile(appFS, filepath.Join(specTable.Name, local.ResourceSpecFileName), []byte("name:a"), 0644)
			afero.WriteFile(appFS, filepath.Join(specTable.Name, "query.sql"), []byte(specTable.Assets["query.sql"]), 0644)

			repo := local.NewResourceSpecRepository(appFS, datastorer)
			_, err := repo.GetByName(specTable.Name)
			assert.NotNil(t, err)
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		content := []string{
			`version: 1
name: proj.datas.test
type: table
spec:
 labels:
   one: valueone
   two: valuetwo
 description: some description
 schema:
 - name: col1
   type: integer`,
			`version: 1
name: proj.datas
type: dataset
spec:
 labels:
   one: valueone
   two: valuetwo
 description: some description`,
		}
		resSpecs := []models.ResourceSpec{
			{
				Version:   1,
				Name:      "proj.datas.test",
				Type:      models.ResourceTypeTable,
				Datastore: datastorer,
				Spec:      nil,
				Assets:    map[string]string{},
			},
			{
				Version:   1,
				Name:      "proj.datas",
				Type:      models.ResourceTypeDataset,
				Datastore: datastorer,
				Spec:      nil,
				Assets:    map[string]string{},
			},
		}

		dsTypeTableAdapter.On("ToYaml", resSpecs[0]).Return([]byte(content[0]), nil)
		dsTypeTableAdapter.On("ToYaml", resSpecs[1]).Return([]byte(content[1]), nil)
		dsTypeTableAdapter.On("FromYaml", []byte(content[0])).Return(resSpecs[0], nil)
		dsTypeTableAdapter.On("FromYaml", []byte(content[1])).Return(resSpecs[1], nil)

		t.Run("should read and parse all files under ${ROOT}", func(t *testing.T) {
			// create test files and directories
			appFS := afero.NewMemMapFs()
			for idx, resSpec := range resSpecs {
				appFS.MkdirAll(resSpec.Name, 0755)
				afero.WriteFile(appFS, filepath.Join(resSpec.Name, local.ResourceSpecFileName), []byte(content[idx]), 0644)
			}

			repo := local.NewResourceSpecRepository(appFS, datastorer)
			result, err := repo.GetAll()
			assert.Nil(t, err)
			assert.Equal(t, len(resSpecs), len(result))

			// sort result
			sort.Slice(result, func(i, j int) bool { return result[i].Name > result[j].Name })
			assert.Equal(t, resSpecs, result)
		})
		t.Run("should return ErrNoResources if the root directory does not exist", func(t *testing.T) {
			repo := local.NewResourceSpecRepository(afero.NewMemMapFs(), datastorer)
			_, err := repo.GetAll()
			assert.Equal(t, models.ErrNoResources, err)
		})
		t.Run("should return ErrNoResources if the root directory has no files", func(t *testing.T) {
			appFS := afero.NewMemMapFs()
			appFS.MkdirAll("test", 0755)

			repo := local.NewResourceSpecRepository(appFS, datastorer)
			_, err := repo.GetAll()
			assert.Equal(t, models.ErrNoResources, err)
		})
		t.Run("should use cache to return specs if called more than once", func(t *testing.T) {
			// create test files and directories
			appFS := afero.NewMemMapFs()
			for idx, resSpec := range resSpecs {
				appFS.MkdirAll(resSpec.Name, 0755)
				afero.WriteFile(appFS, filepath.Join(resSpec.Name, local.ResourceSpecFileName), []byte(content[idx]), 0644)
			}

			repo := local.NewResourceSpecRepository(appFS, datastorer)
			result, err := repo.GetAll()
			sort.Slice(result, func(i, j int) bool { return result[i].Name > result[j].Name })
			assert.Nil(t, err)
			assert.Equal(t, resSpecs, result)

			// clear fs
			assert.Nil(t, appFS.RemoveAll("."))

			resultAgain, err := repo.GetAll()
			assert.Nil(t, err)
			assert.Equal(t, len(result), len(resultAgain))
		})
	})
}
