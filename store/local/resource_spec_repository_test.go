package local_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"

	"github.com/odpf/optimus/core/fs"
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
			fs := new(mock.FileSystem)
			dst := new(mock.File)
			buf := new(bytes.Buffer)
			ast := new(mock.File)
			bufAst := new(bytes.Buffer)

			fs.On("OpenForWrite", filepath.Join(specTable.Name, "query.sql")).Return(ast, nil)
			fs.On("OpenForWrite", filepath.Join(specTable.Name, local.ResourceSpecFileName)).Return(dst, nil)
			defer fs.AssertExpectations(t)

			ast.On("Write").Return(bufAst)
			ast.On("Close").Return(nil)
			defer ast.AssertExpectations(t)

			dst.On("Write").Return(buf)
			dst.On("Close").Return(nil)
			defer dst.AssertExpectations(t)

			repo := local.NewResourceSpecRepository(fs, datastorer)
			err := repo.Save(specTable)
			assert.Nil(t, err)
			assert.Equal(t, testResourceContents, buf.String())
			asset, _ := specTable.Assets.GetByName("query.sql")
			assert.Equal(t, asset, bufAst.String())
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
		t.Run("should return error if opening the file fails", func(t *testing.T) {
			fs := new(mock.FileSystem)
			fsErr := errors.New("I/O error")
			fs.On("OpenForWrite", filepath.Join(specTable.Name, "query.sql")).Return(new(mock.File), fsErr)
			defer fs.AssertExpectations(t)

			repo := local.NewResourceSpecRepository(fs, datastorer)
			err := repo.Save(specTable)
			assert.Equal(t, fsErr, err)
		})
	})

	t.Run("GetByName", func(t *testing.T) {
		t.Run("should open the file ${ROOT}/${name}.yaml and parse its contents", func(t *testing.T) {
			jobfile := new(mock.File)
			assetfile := new(mock.File)

			jobdr := new(mock.File)
			jobdr.On("Readdirnames", -1).Return([]string{local.ResourceSpecFileName, "query.sql"}, nil)
			jobdr.On("IsDir").Return(true, nil)
			jobdr.On("Close").Return(nil)
			defer jobdr.AssertExpectations(t)

			// job file
			jobfile.On("Read").Return(bytes.NewBufferString(testResourceContents))
			jobfile.On("Close").Return(nil)
			defer jobfile.AssertExpectations(t)

			// single asset file
			assetfile.On("Read").Return(bytes.NewBufferString(specTable.Assets["query.sql"]))
			assetfile.On("IsDir").Return(false, nil)
			assetfile.On("Close").Return(nil)
			defer assetfile.AssertExpectations(t)

			// mock for reading the directory
			rootdir := new(mock.File)
			rootdir.On("Readdirnames", -1).Return([]string{specTable.Name}, nil)
			rootdir.On("Close").Return(nil)
			defer rootdir.AssertExpectations(t)

			// root dir reading
			fsm := new(mock.FileSystem)
			fsm.On("Open", ".").Return(rootdir, nil)
			fsm.On("Open", filepath.Join(".", local.ResourceSpecFileName)).Return(rootdir, fs.ErrNoSuchFile)
			fsm.On("Open", specTable.Name).Return(jobdr, nil)
			fsm.On("Open", filepath.Join(specTable.Name, local.ResourceSpecFileName)).Return(jobfile, nil).Once()
			fsm.On("Open", filepath.Join(specTable.Name, "query.sql")).Return(assetfile, nil)
			defer fsm.AssertExpectations(t)

			repo := local.NewResourceSpecRepository(fsm, datastorer)
			returnedSpec, err := repo.GetByName(specTable.Name)
			assert.Nil(t, err)
			assert.Equal(t, specTable, returnedSpec)
		})
		t.Run("should use cache if file is requested more than once", func(t *testing.T) {
			jobfile := new(mock.File)
			assetfile := new(mock.File)

			jobdr := new(mock.File)
			jobdr.On("Readdirnames", -1).Return([]string{local.ResourceSpecFileName, "query.sql"}, nil)
			jobdr.On("IsDir").Return(true, nil)
			jobdr.On("Close").Return(nil)
			defer jobdr.AssertExpectations(t)

			// job file
			jobfile.On("Read").Return(bytes.NewBufferString(testResourceContents))
			jobfile.On("Close").Return(nil)
			defer jobfile.AssertExpectations(t)

			// single asset file
			assetfile.On("Read").Return(bytes.NewBufferString(specTable.Assets["query.sql"]))
			assetfile.On("IsDir").Return(false, nil)
			assetfile.On("Close").Return(nil)
			defer assetfile.AssertExpectations(t)

			// mock for reading the directory
			rootdir := new(mock.File)
			rootdir.On("Readdirnames", -1).Return([]string{specTable.Name}, nil)
			rootdir.On("Close").Return(nil)
			defer rootdir.AssertExpectations(t)

			// root dir reading
			fsm := new(mock.FileSystem)
			fsm.On("Open", ".").Return(rootdir, nil)
			fsm.On("Open", filepath.Join(".", local.ResourceSpecFileName)).Return(rootdir, fs.ErrNoSuchFile)
			fsm.On("Open", specTable.Name).Return(jobdr, nil)
			fsm.On("Open", filepath.Join(specTable.Name, local.ResourceSpecFileName)).Return(jobfile, nil).Once()
			fsm.On("Open", filepath.Join(specTable.Name, "query.sql")).Return(assetfile, nil)
			defer fsm.AssertExpectations(t)

			repo := local.NewResourceSpecRepository(fsm, datastorer)
			returnedSpec, err := repo.GetByName(specTable.Name)
			assert.Nil(t, err)
			assert.Equal(t, specTable, returnedSpec)

			returnedSpecAgain, err := repo.GetByName(specTable.Name)
			assert.Nil(t, err)
			assert.Equal(t, specTable, returnedSpecAgain)
		})
		t.Run("should return ErrNoResources in case no job folder exist", func(t *testing.T) {
			// mock for reading the directory
			rootdir := new(mock.File)
			rootdir.On("Readdirnames", -1).Return([]string{}, nil)
			rootdir.On("Close").Return(nil)
			defer rootdir.AssertExpectations(t)

			mfs := new(mock.FileSystem)
			mfs.On("Open", ".").Return(rootdir, nil)
			mfs.On("Open", filepath.Join(".", local.ResourceSpecFileName)).Return(rootdir, fs.ErrNoSuchFile)
			defer mfs.AssertExpectations(t)

			repo := local.NewResourceSpecRepository(mfs, datastorer)
			_, err := repo.GetByName(specTable.Name)
			assert.Equal(t, models.ErrNoResources, err)
		})
		t.Run("should return ErrNoResources in case the folder exist but no resource file exist", func(t *testing.T) {
			// mock for reading the directory
			rootdir := new(mock.File)
			rootdir.On("Readdirnames", -1).Return([]string{specTable.Name}, nil)
			rootdir.On("Close").Return(nil)
			defer rootdir.AssertExpectations(t)

			jobdr := new(mock.File)
			jobdr.On("Readdirnames", -1).Return([]string{}, nil)
			jobdr.On("IsDir").Return(true, nil)
			jobdr.On("Close").Return(nil)
			defer jobdr.AssertExpectations(t)

			fsm := new(mock.FileSystem)
			fsm.On("Open", ".").Return(rootdir, nil).Once()
			fsm.On("Open", filepath.Join(".", local.ResourceSpecFileName)).Return(rootdir, fs.ErrNoSuchFile)
			fsm.On("Open", specTable.Name).Return(jobdr, nil)
			fsm.On("Open", filepath.Join(specTable.Name, local.ResourceSpecFileName)).Return(jobdr, fs.ErrNoSuchFile)
			defer fsm.AssertExpectations(t)

			repo := local.NewResourceSpecRepository(fsm, datastorer)
			_, err := repo.GetByName(specTable.Name)
			assert.Equal(t, models.ErrNoResources, err)
		})
		t.Run("should return an error if name is empty", func(t *testing.T) {
			repo := local.NewResourceSpecRepository(new(mock.FileSystem), nil)
			_, err := repo.GetByName("")
			assert.NotNil(t, err)
		})
		t.Run("should return error if yaml source is incorrect and failed to validate", func(t *testing.T) {
			jobfile := new(mock.File)

			jobdr := new(mock.File)
			jobdr.On("Readdirnames", -1).Return([]string{local.ResourceSpecFileName}, nil)
			jobdr.On("IsDir").Return(true, nil)
			jobdr.On("Close").Return(nil)
			defer jobdr.AssertExpectations(t)

			// job file
			jobfile.On("Read").Return(bytes.NewBufferString("name:a"))
			jobfile.On("Close").Return(nil)
			defer jobfile.AssertExpectations(t)

			// mock for reading the directory
			rootdir := new(mock.File)
			rootdir.On("Readdirnames", -1).Return([]string{specTable.Name}, nil)
			rootdir.On("Close").Return(nil)
			defer rootdir.AssertExpectations(t)

			// root dir reading
			fsm := new(mock.FileSystem)
			fsm.On("Open", ".").Return(rootdir, nil)
			fsm.On("Open", specTable.Name).Return(jobdr, nil)
			fsm.On("Open", filepath.Join(specTable.Name, local.ResourceSpecFileName)).Return(jobfile, nil).Once()
			defer fsm.AssertExpectations(t)

			repo := local.NewResourceSpecRepository(fsm, datastorer)
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
			fsm := new(mock.FileSystem)
			defer fsm.AssertExpectations(t)

			for idx, resSpec := range resSpecs {
				jobfile := new(mock.File)
				jobdr := new(mock.File)

				jobdr.On("Readdirnames", -1).Return([]string{local.ResourceSpecFileName}, nil)
				jobdr.On("IsDir").Return(true, nil)
				jobdr.On("Close").Return(nil)
				defer jobdr.AssertExpectations(t)
				fsm.On("Open", resSpec.Name).Return(jobdr, nil)

				fsm.On("Open", filepath.Join(resSpec.Name, local.ResourceSpecFileName)).Return(jobfile, nil).Once()

				jobfile.On("Read").Return(bytes.NewBufferString(content[idx]))
				jobfile.On("Close").Return(nil)
				defer jobfile.AssertExpectations(t)
			}

			// mock for reading the directory
			dir := new(mock.File)
			dir.On("Readdirnames", -1).Return([]string{resSpecs[0].Name, resSpecs[1].Name}, nil)
			dir.On("Close").Return(nil)
			defer dir.AssertExpectations(t)

			fsm.On("Open", ".").Return(dir, nil)
			fsm.On("Open", filepath.Join(".", local.ResourceSpecFileName)).Return(dir, fs.ErrNoSuchFile)

			repo := local.NewResourceSpecRepository(fsm, datastorer)
			result, err := repo.GetAll()
			assert.Nil(t, err)
			assert.Equal(t, len(resSpecs), len(result))

			// sort result
			sort.Slice(result, func(i, j int) bool { return result[i].Name > result[j].Name })
			assert.Equal(t, resSpecs, result)
		})
		t.Run("should return ErrNoResources if the root directory does not exist", func(t *testing.T) {
			mfs := new(mock.FileSystem)
			mfs.On("Open", ".").Return(new(mock.File), fs.ErrNoSuchFile)
			repo := local.NewResourceSpecRepository(mfs, datastorer)
			_, err := repo.GetAll()
			assert.Equal(t, models.ErrNoResources, err)
		})
		t.Run("should return ErrNoResources if the root directory has no files", func(t *testing.T) {
			dir := new(mock.File)
			dir.On("Readdirnames", -1).Return([]string{}, nil)
			dir.On("Close").Return(nil)
			defer dir.AssertExpectations(t)

			mfs := new(mock.FileSystem)
			mfs.On("Open", ".").Return(dir, nil)
			mfs.On("Open", filepath.Join(".", local.ResourceSpecFileName)).Return(dir, fs.ErrNoSuchFile)
			defer mfs.AssertExpectations(t)

			repo := local.NewResourceSpecRepository(mfs, datastorer)
			_, err := repo.GetAll()
			assert.Equal(t, models.ErrNoResources, err)
		})
		t.Run("should return an error if reading the directory fails", func(t *testing.T) {
			readErr := errors.New("not a directory")
			dir := new(mock.File)
			dir.On("Readdirnames", -1).Return([]string{}, readErr)
			defer dir.AssertExpectations(t)

			mfs := new(mock.FileSystem)
			mfs.On("Open", ".").Return(dir, nil)
			defer mfs.AssertExpectations(t)

			repo := local.NewResourceSpecRepository(mfs, datastorer)
			_, err := repo.GetAll()
			assert.Equal(t, readErr, err)
		})
		t.Run("should return an error if reading a job file fails", func(t *testing.T) {
			dirName := "proj.data.tab"

			mfs := new(mock.FileSystem)
			jobFile := new(mock.File)
			jobDir := new(mock.File)
			dir := new(mock.File)

			mfs.On("Open", ".").Return(dir, nil)
			defer mfs.AssertExpectations(t)

			dir.On("Readdirnames", -1).Return([]string{dirName}, nil)
			dir.On("Close").Return(nil)
			defer dir.AssertExpectations(t)

			mfs.On("Open", dirName).Return(jobDir, nil)
			jobDir.On("Readdirnames", -1).Return([]string{local.ResourceSpecFileName}, nil)
			jobDir.On("IsDir").Return(true, nil)
			jobDir.On("Close").Return(nil)
			defer jobDir.AssertExpectations(t)

			mfs.On("Open", filepath.Join(dirName, local.ResourceSpecFileName)).Return(jobFile, nil)
			jobFile.On("Read").Return(new(badReader))
			defer jobFile.AssertExpectations(t)

			repo := local.NewResourceSpecRepository(mfs, datastorer)
			_, err := repo.GetAll()
			assert.NotNil(t, err)
		})
		t.Run("should use cache to return specs if called more than once", func(t *testing.T) {
			fsm := new(mock.FileSystem)
			defer fsm.AssertExpectations(t)

			for idx, resSpec := range resSpecs {
				jobfile := new(mock.File)
				jobdr := new(mock.File)

				jobdr.On("Readdirnames", -1).Return([]string{local.ResourceSpecFileName}, nil)
				jobdr.On("IsDir").Return(true, nil)

				jobdr.On("Close").Return(nil)
				defer jobdr.AssertExpectations(t)
				fsm.On("Open", resSpec.Name).Return(jobdr, nil)

				fsm.On("Open", filepath.Join(resSpec.Name, local.ResourceSpecFileName)).Return(jobfile, nil).Once()

				jobfile.On("Read").Return(bytes.NewBufferString(content[idx]))
				jobfile.On("Close").Return(nil)
				defer jobfile.AssertExpectations(t)
			}

			// mock for reading the directory
			dir := new(mock.File)
			dir.On("Readdirnames", -1).Return([]string{resSpecs[0].Name, resSpecs[1].Name}, nil)
			dir.On("Close").Return(nil)
			defer dir.AssertExpectations(t)

			fsm.On("Open", ".").Return(dir, nil)
			fsm.On("Open", filepath.Join(".", local.ResourceSpecFileName)).Return(dir, fs.ErrNoSuchFile)

			repo := local.NewResourceSpecRepository(fsm, datastorer)
			result, err := repo.GetAll()
			sort.Slice(result, func(i, j int) bool { return result[i].Name > result[j].Name })
			assert.Nil(t, err)
			assert.Equal(t, resSpecs, result)

			resultAgain, err := repo.GetAll()
			assert.Nil(t, err)
			assert.Equal(t, len(result), len(resultAgain))
		})
	})
}
