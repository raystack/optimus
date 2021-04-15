package local_test

import (
	"bytes"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v2"

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

var testJobContents = `version: 1
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
- job: bar
hooks: []
`

func TestJobSpecRepository(t *testing.T) {
	// prepare adapter
	execUnit := new(mock.Transformer)
	execUnit.On("Name").Return("foo")
	allTasksRepo := new(mock.SupportedTransformationRepo)
	allTasksRepo.On("GetByName", "foo").Return(execUnit, nil)
	adapter := local.NewJobSpecAdapter(allTasksRepo, nil)

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
			Config: yaml.MapSlice{
				{
					Key:   "table",
					Value: "tab1",
				},
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
		Dependencies: []local.JobDependency{
			{
				JobName: "bar",
				Type:    models.JobSpecDependencyTypeIntra.String(),
			},
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
			Unit: execUnit,
			Window: models.JobSpecTaskWindow{
				Offset:     0,
				Size:       time.Hour * 24,
				TruncateTo: "d",
			},
			Config: models.JobSpecConfigs{
				{
					Name:  "table",
					Value: "tab1",
				},
			},
		},
		Dependencies: map[string]models.JobSpecDependency{
			"bar": {},
		},
		Assets: models.JobAssets{}.FromMap(map[string]string{
			"query.sql": "select * from 1",
		}),
	}

	spec2 := models.JobSpec{
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
			Unit: execUnit,
			Window: models.JobSpecTaskWindow{
				Offset:     0,
				Size:       time.Hour * 24,
				TruncateTo: "d",
			},
			Config: models.JobSpecConfigs{
				{
					Name:  "table",
					Value: "tab1",
				},
			},
		},
		Labels: map[string]string{},
		Dependencies: map[string]models.JobSpecDependency{
			"bar": {Type: models.JobSpecDependencyTypeIntra},
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

			fs.On("OpenForWrite", filepath.Join(spec.Name, local.AssetFolderName, "query.sql")).Return(ast, nil)
			fs.On("OpenForWrite", filepath.Join(spec.Name, local.JobSpecFileName)).Return(dst, nil)
			defer fs.AssertExpectations(t)

			ast.On("Write").Return(bufAst)
			ast.On("Close").Return(nil)
			defer ast.AssertExpectations(t)

			dst.On("Write").Return(buf)
			dst.On("Close").Return(nil)
			defer dst.AssertExpectations(t)

			repo := local.NewJobSpecRepository(fs, adapter)
			err := repo.Save(spec)
			assert.Nil(t, err)
			assert.Equal(t, testJobContents, buf.String())
			asset, _ := spec.Assets.GetByName("query.sql")
			assert.Equal(t, asset.Value, bufAst.String())
		})
		t.Run("should return error if task is empty", func(t *testing.T) {
			repo := local.NewJobSpecRepository(nil, adapter)
			err := repo.Save(models.JobSpec{Name: "foo"})
			assert.NotNil(t, err)
		})
		t.Run("should return error if name is empty", func(t *testing.T) {
			repo := local.NewJobSpecRepository(nil, adapter)
			err := repo.Save(models.JobSpec{Task: models.JobSpecTask{
				Unit: execUnit,
			}})
			assert.NotNil(t, err)
		})
		t.Run("should return error if opening the file fails", func(t *testing.T) {
			fs := new(mock.FileSystem)
			fsErr := errors.New("I/O error")
			fs.On("OpenForWrite", filepath.Join(jobConfig.Name, local.AssetFolderName, "query.sql")).Return(new(mock.File), fsErr)
			defer fs.AssertExpectations(t)

			repo := local.NewJobSpecRepository(fs, adapter)
			err := repo.Save(spec)
			assert.Equal(t, fsErr, err)
		})
		t.Run("should update the file with hooks in the same spec ${ROOT}/${name}.yaml", func(t *testing.T) {
			fs := new(mock.FileSystem)
			dst := new(mock.File)       // job spec file
			buf := new(bytes.Buffer)    // job buffer
			ast := new(mock.File)       // asset file
			bufAst := new(bytes.Buffer) // asset buffer

			defer fs.AssertExpectations(t)
			defer ast.AssertExpectations(t)
			defer dst.AssertExpectations(t)

			fs.On("OpenForWrite", filepath.Join(spec.Name, local.AssetFolderName, "query.sql")).Return(ast, nil)
			ast.On("Write").Return(bufAst)
			ast.On("Close").Return(nil)

			fs.On("OpenForWrite", filepath.Join(spec.Name, local.JobSpecFileName)).Return(dst, nil)
			dst.On("Write").Return(buf)
			dst.On("Close").Return(nil)

			repo := local.NewJobSpecRepository(fs, adapter)
			err := repo.Save(spec)
			assert.Nil(t, err)
			assert.Equal(t, testJobContents, buf.String())
			asset, _ := spec.Assets.GetByName("query.sql")
			assert.Equal(t, asset.Value, bufAst.String())

			// update the spec.
			hookName := "g-hook"
			hookUnit1 := new(mock.HookUnit)
			hookUnit1.On("Name").Return(hookName)
			allHooksRepo := new(mock.SupportedHookRepo)
			allHooksRepo.On("GetByName", hookName).Return(hookUnit1, nil)
			adapterNew := local.NewJobSpecAdapter(allTasksRepo, allHooksRepo)

			specCopy := spec
			specCopy.Hooks = []models.JobSpecHook{
				{Config: models.JobSpecConfigs{
					{
						Name:  "key",
						Value: "value",
					},
				}, Unit: hookUnit1},
			}

			fsNew := new(mock.FileSystem)
			dstNew := new(mock.File)
			bufNew := new(bytes.Buffer)
			astNew := new(mock.File)       // asset file
			bufAstNew := new(bytes.Buffer) // asset buffer
			defer fsNew.AssertExpectations(t)
			defer dstNew.AssertExpectations(t)
			defer astNew.AssertExpectations(t)

			fsNew.On("OpenForWrite", filepath.Join(spec.Name, local.AssetFolderName, "query.sql")).Return(astNew, nil)
			astNew.On("Write").Return(bufAstNew)
			astNew.On("Close").Return(nil)

			fsNew.On("OpenForWrite", filepath.Join(specCopy.Name, local.JobSpecFileName)).Return(dstNew, nil)
			dstNew.On("Write").Return(bufNew)
			dstNew.On("Close").Return(nil)

			repoNew := local.NewJobSpecRepository(fsNew, adapterNew)
			err = repoNew.Save(specCopy)
			assert.Nil(t, err)
			testJobContentsNew := strings.ReplaceAll(testJobContents, "hooks: []\n",
				"hooks:\n- name: g-hook\n  config:\n    key: value\n")
			assert.Equal(t, testJobContentsNew, bufNew.String())
		})
	})

	t.Run("GetByName", func(t *testing.T) {
		t.Run("should open the file ${ROOT}/${name}.yaml and parse its contents", func(t *testing.T) {
			jobfile := new(mock.File)
			assetDirfile := new(mock.File)
			assetfile := new(mock.File)

			jobdr := new(mock.File)
			jobdr.On("Readdirnames", -1).Return([]string{local.JobSpecFileName, local.AssetFolderName}, nil)
			jobdr.On("IsDir").Return(true, nil)
			jobdr.On("Close").Return(nil)
			defer jobdr.AssertExpectations(t)

			// job file
			jobfile.On("Read").Return(bytes.NewBufferString(testJobContents))
			jobfile.On("Close").Return(nil)
			defer jobfile.AssertExpectations(t)

			// dir where assets are stored
			assetDirfile.On("Readdirnames", -1).Return([]string{"query.sql"}, nil)
			assetDirfile.On("Close").Return(nil)
			defer assetDirfile.AssertExpectations(t)

			// single asset file
			assetfile.On("Read").Return(bytes.NewBufferString(jobConfig.Asset["query.sql"]))
			assetfile.On("IsDir").Return(false, nil)
			assetfile.On("Close").Return(nil)
			defer assetfile.AssertExpectations(t)

			// mock for reading the directory
			rootdir := new(mock.File)
			rootdir.On("Readdirnames", -1).Return([]string{spec.Name}, nil)
			rootdir.On("Close").Return(nil)
			defer rootdir.AssertExpectations(t)

			// root dir reading
			fsm := new(mock.FileSystem)
			fsm.On("Open", ".").Return(rootdir, nil)
			fsm.On("Open", filepath.Join(".", local.JobSpecFileName)).Return(rootdir, fs.ErrNoSuchFile)
			fsm.On("Open", spec.Name).Return(jobdr, nil)
			fsm.On("Open", filepath.Join(spec.Name, local.JobSpecFileName)).Return(jobfile, nil).Once()
			fsm.On("Open", filepath.Join(spec.Name, local.AssetFolderName)).Return(assetDirfile, nil)
			fsm.On("Open", filepath.Join(spec.Name, local.AssetFolderName, "query.sql")).Return(assetfile, nil)
			defer fsm.AssertExpectations(t)

			repo := local.NewJobSpecRepository(fsm, adapter)
			returnedSpec, err := repo.GetByName(spec.Name)
			assert.Nil(t, err)
			assert.Equal(t, spec2, returnedSpec)
		})
		t.Run("should use cache if file is requested more than once", func(t *testing.T) {
			jobfile := new(mock.File)
			assetDirfile := new(mock.File)
			assetfile := new(mock.File)

			jobdr := new(mock.File)
			jobdr.On("Readdirnames", -1).Return([]string{local.JobSpecFileName, local.AssetFolderName}, nil)
			jobdr.On("IsDir").Return(true, nil)
			jobdr.On("Close").Return(nil)
			defer jobdr.AssertExpectations(t)

			// job file
			jobfile.On("Read").Return(bytes.NewBufferString(testJobContents))
			jobfile.On("Close").Return(nil)
			defer jobfile.AssertExpectations(t)

			// dir where assets are stored
			assetDirfile.On("Readdirnames", -1).Return([]string{"query.sql"}, nil)
			assetDirfile.On("Close").Return(nil)
			defer assetDirfile.AssertExpectations(t)

			// single asset file
			assetfile.On("Read").Return(bytes.NewBufferString(jobConfig.Asset["query.sql"]))
			assetfile.On("IsDir").Return(false, nil)
			assetfile.On("Close").Return(nil)
			defer assetfile.AssertExpectations(t)

			// mock for reading the directory
			rootdir := new(mock.File)
			rootdir.On("Readdirnames", -1).Return([]string{spec.Name}, nil)
			rootdir.On("Close").Return(nil)
			defer rootdir.AssertExpectations(t)

			// root dir reading
			fsm := new(mock.FileSystem)
			fsm.On("Open", ".").Return(rootdir, nil).Once()
			fsm.On("Open", filepath.Join(".", local.JobSpecFileName)).Return(rootdir, fs.ErrNoSuchFile)
			fsm.On("Open", spec.Name).Return(jobdr, nil)
			fsm.On("Open", filepath.Join(spec.Name, local.JobSpecFileName)).Return(jobfile, nil)
			fsm.On("Open", filepath.Join(spec.Name, local.AssetFolderName)).Return(assetDirfile, nil)
			fsm.On("Open", filepath.Join(spec.Name, local.AssetFolderName, "query.sql")).Return(assetfile, nil)
			defer fsm.AssertExpectations(t)

			repo := local.NewJobSpecRepository(fsm, adapter)
			returnedSpec, err := repo.GetByName(spec.Name)
			assert.Nil(t, err)
			assert.Equal(t, spec2, returnedSpec)

			returnedSpecAgain, err := repo.GetByName(spec.Name)
			assert.Nil(t, err)
			assert.Equal(t, spec2, returnedSpecAgain)
		})
		t.Run("should return ErrNoDAGSpecs in case no job folder exist", func(t *testing.T) {
			// mock for reading the directory
			rootdir := new(mock.File)
			rootdir.On("Readdirnames", -1).Return([]string{}, nil)
			rootdir.On("Close").Return(nil)
			defer rootdir.AssertExpectations(t)

			mfs := new(mock.FileSystem)
			mfs.On("Open", ".").Return(rootdir, nil)
			mfs.On("Open", filepath.Join(".", local.JobSpecFileName)).Return(rootdir, fs.ErrNoSuchFile)
			defer mfs.AssertExpectations(t)

			repo := local.NewJobSpecRepository(mfs, adapter)
			_, err := repo.GetByName(spec.Name)
			assert.Equal(t, models.ErrNoDAGSpecs, err)
		})
		t.Run("should return ErrNoDAGSpecs in case the job folder exist but no job file exist", func(t *testing.T) {
			// mock for reading the directory
			rootdir := new(mock.File)
			rootdir.On("Readdirnames", -1).Return([]string{spec.Name}, nil)
			rootdir.On("Close").Return(nil)
			defer rootdir.AssertExpectations(t)

			jobdr := new(mock.File)
			jobdr.On("Readdirnames", -1).Return([]string{}, nil)
			jobdr.On("IsDir").Return(true, nil)
			jobdr.On("Close").Return(nil)
			defer jobdr.AssertExpectations(t)

			fsm := new(mock.FileSystem)
			fsm.On("Open", ".").Return(rootdir, nil).Once()
			fsm.On("Open", filepath.Join(".", local.JobSpecFileName)).Return(rootdir, fs.ErrNoSuchFile)
			fsm.On("Open", spec.Name).Return(jobdr, nil)
			fsm.On("Open", filepath.Join(spec.Name, local.JobSpecFileName)).Return(jobdr, fs.ErrNoSuchFile)
			defer fsm.AssertExpectations(t)

			repo := local.NewJobSpecRepository(fsm, adapter)
			_, err := repo.GetByName(spec.Name)
			assert.Equal(t, models.ErrNoDAGSpecs, err)
		})
		t.Run("should return an error if jobName is empty", func(t *testing.T) {
			repo := local.NewJobSpecRepository(new(mock.FileSystem), nil)
			_, err := repo.GetByName("")
			assert.NotNil(t, err)
		})
		t.Run("should return error if yaml source is incorrect and failed to validate", func(t *testing.T) {
			jobfile := new(mock.File)

			jobdr := new(mock.File)
			jobdr.On("Readdirnames", -1).Return([]string{local.JobSpecFileName}, nil)
			jobdr.On("IsDir").Return(true, nil)
			jobdr.On("Close").Return(nil)
			defer jobdr.AssertExpectations(t)

			// job file
			jobfile.On("Read").Return(bytes.NewBufferString("name:a"))
			jobfile.On("Close").Return(nil)
			defer jobfile.AssertExpectations(t)

			// mock for reading the directory
			rootdir := new(mock.File)
			rootdir.On("Readdirnames", -1).Return([]string{spec.Name}, nil)
			rootdir.On("Close").Return(nil)
			defer rootdir.AssertExpectations(t)

			// root dir reading
			fsm := new(mock.FileSystem)
			fsm.On("Open", ".").Return(rootdir, nil)
			fsm.On("Open", spec.Name).Return(jobdr, nil)
			fsm.On("Open", filepath.Join(spec.Name, local.JobSpecFileName)).Return(jobfile, nil).Once()
			defer fsm.AssertExpectations(t)

			repo := local.NewJobSpecRepository(fsm, adapter)
			_, err := repo.GetByName(spec.Name)
			assert.NotNil(t, err)
			//t.Error(err)
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
    truncate_to: d
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
dependencies: []
hooks: []
`,
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
					Unit:   execUnit,
					Config: models.JobSpecConfigs{},
					Window: models.JobSpecTaskWindow{
						Offset:     0,
						Size:       time.Hour * 24,
						TruncateTo: "d",
					},
				},
				Dependencies: map[string]models.JobSpecDependency{},
				Assets:       models.JobAssets{},
				Labels:       map[string]string{},
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
					Unit:   execUnit,
					Config: models.JobSpecConfigs{},
					Window: models.JobSpecTaskWindow{
						Offset:     0,
						Size:       time.Hour * 24,
						TruncateTo: "d",
					},
				},
				Dependencies: map[string]models.JobSpecDependency{},
				Assets:       models.JobAssets{},
				Labels:       map[string]string{},
			},
		}

		t.Run("should read and parse all files under ${ROOT}", func(t *testing.T) {
			fsm := new(mock.FileSystem)
			defer fsm.AssertExpectations(t)

			for idx, jobspec := range jobspecs {
				jobfile := new(mock.File)
				jobdr := new(mock.File)
				assetDirfile := new(mock.File)

				jobdr.On("Readdirnames", -1).Return([]string{local.JobSpecFileName, local.AssetFolderName}, nil)
				jobdr.On("IsDir").Return(true, nil)
				jobdr.On("Close").Return(nil)
				defer jobdr.AssertExpectations(t)
				fsm.On("Open", jobspec.Name).Return(jobdr, nil)

				fsm.On("Open", filepath.Join(jobspec.Name, local.JobSpecFileName)).Return(jobfile, nil).Once()
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
			fsm.On("Open", filepath.Join(".", local.JobSpecFileName)).Return(dir, fs.ErrNoSuchFile)

			repo := local.NewJobSpecRepository(fsm, adapter)
			result, err := repo.GetAll()
			assert.Nil(t, err)
			assert.Equal(t, len(jobspecs), len(result))

			// sort result
			sort.Slice(result, func(i, j int) bool { return result[i].Name > result[j].Name })
			assert.Equal(t, jobspecs, result)
		})
		t.Run("should return ErrNoSpecsFound if the root directory does not exist", func(t *testing.T) {
			mfs := new(mock.FileSystem)
			mfs.On("Open", ".").Return(new(mock.File), fs.ErrNoSuchFile)
			repo := local.NewJobSpecRepository(mfs, adapter)
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
			mfs.On("Open", filepath.Join(".", local.JobSpecFileName)).Return(dir, fs.ErrNoSuchFile)
			defer mfs.AssertExpectations(t)

			repo := local.NewJobSpecRepository(mfs, adapter)
			_, err := repo.GetAll()
			assert.Equal(t, models.ErrNoDAGSpecs, err)
		})
		t.Run("should return an error if reading the directory fails", func(t *testing.T) {
			readErr := errors.New("not a directory")
			dir := new(mock.File)
			dir.On("Readdirnames", -1).Return([]string{}, readErr)
			defer dir.AssertExpectations(t)

			mfs := new(mock.FileSystem)
			mfs.On("Open", ".").Return(dir, nil)
			defer mfs.AssertExpectations(t)

			repo := local.NewJobSpecRepository(mfs, adapter)
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
			jobDir.On("Readdirnames", -1).Return([]string{local.JobSpecFileName}, nil)
			jobDir.On("IsDir").Return(true, nil)
			jobDir.On("Close").Return(nil)
			defer jobDir.AssertExpectations(t)

			mfs.On("Open", filepath.Join(dirName, local.JobSpecFileName)).Return(jobFile, nil)
			jobFile.On("Read").Return(new(badReader))
			jobFile.On("Close").Return(nil)
			defer jobFile.AssertExpectations(t)

			repo := local.NewJobSpecRepository(mfs, adapter)
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

				jobdr.On("Readdirnames", -1).Return([]string{local.JobSpecFileName, local.AssetFolderName}, nil)
				jobdr.On("IsDir").Return(true, nil)

				jobdr.On("Close").Return(nil)
				defer jobdr.AssertExpectations(t)
				fsm.On("Open", jobspec.Name).Return(jobdr, nil)

				fsm.On("Open", filepath.Join(jobspec.Name, local.JobSpecFileName)).Return(jobfile, nil).Once()
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
			fsm.On("Open", filepath.Join(".", local.JobSpecFileName)).Return(dir, fs.ErrNoSuchFile)

			repo := local.NewJobSpecRepository(fsm, adapter)
			result, err := repo.GetAll()
			sort.Slice(result, func(i, j int) bool { return result[i].Name > result[j].Name })
			assert.Nil(t, err)
			assert.Equal(t, jobspecs, result)

			resultAgain, err := repo.GetAll()
			assert.Nil(t, err)
			assert.Equal(t, len(result), len(resultAgain))
		})
	})
}
