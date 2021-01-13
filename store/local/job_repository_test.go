package local_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store/local"

	"github.com/odpf/optimus/core/fs"
	"github.com/odpf/optimus/mock"
)

type badReader int

func (r badReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("bad reader")
}

var testContents = `version: 1
name: test
owner: optimus
schedule:
  start_date: "2020-12-02"
  interval: '@daily'
behavior:
  depends_on_past: false
  catch_up: true
task:
  name: foo
  config:
    table: tab1
  window:
    size: 24h
    offset: "0"
    truncate_to: d
dependencies:
- bar
`

func TestSpecRepository(t *testing.T) {
	jobConfig := local.Job{
		Version: 1,
		Name:    "test",
		Owner:   "optimus",
		Schedule: local.JobSchedule{
			StartDate: "2020-12-02",
			Interval:  "@daily",
		},
		Behavior: local.JobBehavior{
			Catchup:       true,
			DependsOnPast: false,
		},
		Task: local.JobTask{
			Name: "foo",
			Config: map[string]string{
				"table": "tab1",
			},
			Window: local.JobTaskWindow{
				Size:       "24h",
				Offset:     "0",
				TruncateTo: "d",
			},
		},
		Asset: map[string]string{
			"query.sql": "select * from 1",
		},
		Dependencies: []string{
			"bar",
		},
	}
	spec := models.JobSpec{
		Version: 1,
		Name:    "test",
		Owner:   "optimus",
		Schedule: models.JobSpecSchedule{
			StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
			Interval:  "@daily",
		},
		Behavior: models.JobSpecBehavior{
			CatchUp:       true,
			DependsOnPast: false,
		},
		Task: models.JobSpecTask{
			Name: "foo",
			Window: models.JobSpecTaskWindow{
				Offset:     0,
				Size:       time.Hour * 24,
				TruncateTo: "d",
			},
			Config: map[string]string{
				"table": "tab1",
			},
		},
		Dependencies: map[string]models.JobSpecDependency{
			"bar": {},
		},
		Assets: models.JobAssets{}.FromMap(map[string]string{
			"query.sql": "select * from 1",
		}),
	}
	t.Run("Save", func(t *testing.T) {
		t.Run("should write the file to ${ROOT}/${name}.yaml", func(t *testing.T) {
			fs := new(mock.FileSystem)
			dst := new(mock.File)
			buf := new(bytes.Buffer)
			ast := new(mock.File)
			bufAst := new(bytes.Buffer)

			fs.On("Create", filepath.Join(spec.Name, local.AssetFolderName, "query.sql")).Return(ast, nil)
			fs.On("Create", filepath.Join(spec.Name, local.SpecFileName)).Return(dst, nil)
			defer fs.AssertExpectations(t)

			ast.On("Write").Return(bufAst)
			ast.On("Close").Return(nil)
			defer ast.AssertExpectations(t)

			dst.On("Write").Return(buf)
			dst.On("Close").Return(nil)
			defer dst.AssertExpectations(t)

			repo := local.NewJobSpecRepository(fs)
			err := repo.Save(spec)
			assert.Nil(t, err)
			assert.Equal(t, testContents, buf.String())
			asset, _ := spec.Assets.GetByName("query.sql")
			assert.Equal(t, asset.Value, bufAst.String())
		})
		t.Run("should return error if Name is empty", func(t *testing.T) {
			repo := local.NewJobSpecRepository(nil)
			err := repo.Save(models.JobSpec{})
			assert.NotNil(t, err)
		})
		t.Run("should return error if opening the file fails", func(t *testing.T) {
			fs := new(mock.FileSystem)
			fsErr := errors.New("I/O error")
			fs.On("Create", filepath.Join(jobConfig.Name, local.AssetFolderName, "query.sql")).Return(new(mock.File), fsErr)
			defer fs.AssertExpectations(t)

			repo := local.NewJobSpecRepository(fs)
			err := repo.Save(spec)
			assert.Equal(t, fsErr, err)
		})
	})
	t.Run("GetByName", func(t *testing.T) {
		t.Run("should open the file ${ROOT}/${name}.yaml and parse its contents", func(t *testing.T) {
			jobfile := new(mock.File)
			assetfile := new(mock.File)
			assetDirfile := new(mock.File)
			fs := new(mock.FileSystem)

			fs.On("Open", filepath.Join(spec.Name, local.SpecFileName)).Return(jobfile, nil)
			fs.On("Open", filepath.Join(spec.Name, local.AssetFolderName)).Return(assetDirfile, nil)
			fs.On("Open", filepath.Join(spec.Name, local.AssetFolderName, "query.sql")).Return(assetfile, nil)
			defer fs.AssertExpectations(t)

			jobfile.On("Read").Return(bytes.NewBufferString(testContents))
			jobfile.On("Close").Return(nil)
			defer jobfile.AssertExpectations(t)

			assetDirfile.On("Readdirnames", -1).Return([]string{"query.sql"}, nil)
			assetDirfile.On("Close").Return(nil)
			defer assetDirfile.AssertExpectations(t)

			assetfile.On("Read").Return(bytes.NewBufferString(jobConfig.Asset["query.sql"]))
			assetfile.On("Close").Return(nil)
			defer assetfile.AssertExpectations(t)

			repo := local.NewJobSpecRepository(fs)
			returnedSpec, err := repo.GetByName(spec.Name)
			assert.Nil(t, err)
			assert.Equal(t, spec, returnedSpec)
		})
		t.Run("should use cache if file is requested more than once", func(t *testing.T) {
			jobfile := new(mock.File)
			assetfile := new(mock.File)
			assetDirfile := new(mock.File)
			fs := new(mock.FileSystem)

			fs.On("Open", filepath.Join(spec.Name, local.SpecFileName)).Return(jobfile, nil).Once()
			fs.On("Open", filepath.Join(spec.Name, local.AssetFolderName)).Return(assetDirfile, nil)
			fs.On("Open", filepath.Join(spec.Name, local.AssetFolderName, "query.sql")).Return(assetfile, nil)
			defer fs.AssertExpectations(t)

			jobfile.On("Read").Return(bytes.NewBufferString(testContents))
			jobfile.On("Close").Return(nil)
			defer jobfile.AssertExpectations(t)

			assetDirfile.On("Readdirnames", -1).Return([]string{"query.sql"}, nil)
			assetDirfile.On("Close").Return(nil)
			defer assetDirfile.AssertExpectations(t)

			assetfile.On("Read").Return(bytes.NewBufferString(jobConfig.Asset["query.sql"]))
			assetfile.On("Close").Return(nil)
			defer assetfile.AssertExpectations(t)

			repo := local.NewJobSpecRepository(fs)
			returnedSpec, err := repo.GetByName(spec.Name)
			assert.Nil(t, err)
			assert.Equal(t, spec, returnedSpec)

			returnedSpecAgain, err := repo.GetByName(spec.Name)
			assert.Nil(t, err)
			assert.Equal(t, spec, returnedSpecAgain)
		})
		t.Run("should return ErrNoSuchSpec in case the file does not exist", func(t *testing.T) {
			mfs := new(mock.FileSystem)
			mfs.On("Open", filepath.Join(jobConfig.Name, local.SpecFileName)).Return(new(mock.File), fs.ErrNoSuchFile)
			defer mfs.AssertExpectations(t)

			repo := local.NewJobSpecRepository(mfs)
			_, err := repo.GetByName(spec.Name)
			assert.Equal(t, models.ErrNoSuchSpec, err)
		})
		t.Run("should return an error if jobName is empty", func(t *testing.T) {
			repo := local.NewJobSpecRepository(new(mock.FileSystem))
			_, err := repo.GetByName("")
			assert.NotNil(t, err)
		})
		t.Run("should return error if yaml source is incorrect and failed to validate", func(t *testing.T) {
			src := bytes.NewBufferString(`{"foo": {"bar": ["baz"]}}`)
			jobName := "foo"
			file := new(mock.File)
			fs := new(mock.FileSystem)

			fs.On("Open", filepath.Join(jobName, local.SpecFileName)).Return(file, nil)
			defer fs.AssertExpectations(t)

			file.On("Read").Return(src)
			file.On("Close").Return(nil)
			defer file.AssertExpectations(t)

			repo := local.NewJobSpecRepository(fs)
			_, err := repo.GetByName(jobName)
			assert.NotNil(t, err)
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		content := []string{
			`version: 1
name: test
owner: optimus
schedule:
  start_date: "2020-12-02"
  interval: '* * * * *'
behavior:
  depends_on_past: false
  catch_up: true
task:
  name: foo
  window:
    size: 24h
    offset: "0"
    tuncate_to: d
`,
			`version: 1
name: fooo
owner: meee
schedule:
  start_date: "2020-12-01"
  interval: '@daily'
behavior:
  depends_on_past: false
  catch_up: true
task:
  name: foo
  window:
    size: 24h
    offset: "0"
    truncate_to: d
dependencies: []`,
		}
		jobspecs := []models.JobSpec{
			{
				Version: 1,
				Name:    "test",
				Owner:   "optimus",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 2, 0, 0, 0, 0, time.UTC),
					Interval:  "* * * * *",
				},
				Behavior: models.JobSpecBehavior{
					CatchUp:       true,
					DependsOnPast: false,
				},
				Task: models.JobSpecTask{
					Name: "foo",
					Window: models.JobSpecTaskWindow{
						Offset:     0,
						Size:       time.Hour * 24,
						TruncateTo: "d",
					},
				},
				Dependencies: map[string]models.JobSpecDependency{},
				Assets:       models.JobAssets{},
			},
			{
				Version: 1,
				Name:    "fooo",
				Owner:   "meee",
				Schedule: models.JobSpecSchedule{
					StartDate: time.Date(2020, 12, 1, 0, 0, 0, 0, time.UTC),
					Interval:  "@daily",
				},
				Behavior: models.JobSpecBehavior{
					CatchUp:       true,
					DependsOnPast: false,
				},
				Task: models.JobSpecTask{
					Name: "foo",
					Window: models.JobSpecTaskWindow{
						Offset:     0,
						Size:       time.Hour * 24,
						TruncateTo: "d",
					},
				},
				Dependencies: map[string]models.JobSpecDependency{},
				Assets:       models.JobAssets{},
			},
		}

		t.Run("should read and parse all files under ${ROOT}", func(t *testing.T) {
			fsm := new(mock.FileSystem)
			defer fsm.AssertExpectations(t)

			for idx, jobspec := range jobspecs {
				jobfile := new(mock.File)
				jobdr := new(mock.File)
				assetDirfile := new(mock.File)

				jobdr.On("Readdirnames", -1).Return([]string{local.SpecFileName, local.AssetFolderName}, nil)
				jobdr.On("Close").Return(nil)
				defer jobdr.AssertExpectations(t)
				fsm.On("Open", jobspec.Name).Return(jobdr, nil)

				fsm.On("Open", filepath.Join(jobspec.Name, local.SpecFileName)).Return(jobfile, nil).Once()
				fsm.On("Open", filepath.Join(jobspec.Name, local.AssetFolderName)).Return(assetDirfile, nil)

				jobfile.On("Read").Return(bytes.NewBufferString(content[idx]))
				jobfile.On("Close").Return(nil)
				defer jobfile.AssertExpectations(t)

				assetDirfile.On("Readdirnames", -1).Return([]string{}, nil)
				assetDirfile.On("Close").Return(nil)
				defer assetDirfile.AssertExpectations(t)
			}

			// mock for reading the directory
			dir := new(mock.File)
			dir.On("Readdirnames", -1).Return([]string{jobspecs[0].Name, jobspecs[1].Name}, nil)
			dir.On("Close").Return(nil)
			defer dir.AssertExpectations(t)

			fsm.On("Open", ".").Return(dir, nil)
			fsm.On("Open", filepath.Join(".", local.SpecFileName)).Return(dir, fs.ErrNoSuchFile)

			repo := local.NewJobSpecRepository(fsm)
			result, err := repo.GetAll()
			assert.Nil(t, err)
			assert.Equal(t, jobspecs, result)
		})
		t.Run("should return ErrNoSpecsFound if the root directory does not exist", func(t *testing.T) {
			mfs := new(mock.FileSystem)
			mfs.On("Open", ".").Return(new(mock.File), fs.ErrNoSuchFile)
			repo := local.NewJobSpecRepository(mfs)
			_, err := repo.GetAll()
			assert.Equal(t, models.ErrNoDAGSpecs, err)
		})
		t.Run("should return ErrNoSpecsFound if the root directory has no files", func(t *testing.T) {
			dir := new(mock.File)
			dir.On("Readdirnames", -1).Return([]string{}, nil)
			dir.On("Close").Return(nil)
			defer dir.AssertExpectations(t)

			mfs := new(mock.FileSystem)
			mfs.On("Open", ".").Return(dir, nil)
			mfs.On("Open", filepath.Join(".", local.SpecFileName)).Return(dir, fs.ErrNoSuchFile)
			defer mfs.AssertExpectations(t)

			repo := local.NewJobSpecRepository(mfs)
			_, err := repo.GetAll()
			assert.Equal(t, models.ErrNoDAGSpecs, err)
		})
		t.Run("should return an error if reading the directory fails", func(t *testing.T) {
			readErr := errors.New("not a directory")
			dir := new(mock.File)
			dir.On("Readdirnames", -1).Return([]string{}, readErr)
			dir.On("Close").Return(nil)
			defer dir.AssertExpectations(t)

			mfs := new(mock.FileSystem)
			mfs.On("Open", ".").Return(dir, nil)
			defer mfs.AssertExpectations(t)

			repo := local.NewJobSpecRepository(mfs)
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
			jobDir.On("Readdirnames", -1).Return([]string{local.SpecFileName}, nil)
			jobDir.On("Close").Return(nil)
			defer jobDir.AssertExpectations(t)

			mfs.On("Open", filepath.Join(dirName, local.SpecFileName)).Return(jobFile, nil)
			jobFile.On("Read").Return(new(badReader))
			jobFile.On("Close").Return(nil)
			defer jobFile.AssertExpectations(t)

			repo := local.NewJobSpecRepository(mfs)
			_, err := repo.GetAll()
			assert.NotNil(t, err)
		})
		t.Run("should use cache to return specs if called more than once", func(t *testing.T) {
			fsm := new(mock.FileSystem)
			defer fsm.AssertExpectations(t)

			for idx, jobspec := range jobspecs {
				jobfile := new(mock.File)
				jobdr := new(mock.File)
				assetDirfile := new(mock.File)

				jobdr.On("Readdirnames", -1).Return([]string{local.SpecFileName, local.AssetFolderName}, nil)
				jobdr.On("Close").Return(nil)
				defer jobdr.AssertExpectations(t)
				fsm.On("Open", jobspec.Name).Return(jobdr, nil)

				fsm.On("Open", filepath.Join(jobspec.Name, local.SpecFileName)).Return(jobfile, nil).Once()
				fsm.On("Open", filepath.Join(jobspec.Name, local.AssetFolderName)).Return(assetDirfile, nil)

				jobfile.On("Read").Return(bytes.NewBufferString(content[idx]))
				jobfile.On("Close").Return(nil)
				defer jobfile.AssertExpectations(t)

				assetDirfile.On("Readdirnames", -1).Return([]string{}, nil)
				assetDirfile.On("Close").Return(nil)
				defer assetDirfile.AssertExpectations(t)
			}

			// mock for reading the directory
			dir := new(mock.File)
			dir.On("Readdirnames", -1).Return([]string{jobspecs[0].Name, jobspecs[1].Name}, nil)
			dir.On("Close").Return(nil)
			defer dir.AssertExpectations(t)

			fsm.On("Open", ".").Return(dir, nil)
			fsm.On("Open", filepath.Join(".", local.SpecFileName)).Return(dir, fs.ErrNoSuchFile)

			repo := local.NewJobSpecRepository(fsm)
			result, err := repo.GetAll()
			assert.Nil(t, err)
			assert.Equal(t, jobspecs, result)

			resultAgain, err := repo.GetAll()
			assert.Nil(t, err)
			assert.Equal(t, len(result), len(resultAgain))
		})
	})
}
