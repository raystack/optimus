package local_test

import (
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/client/local"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

const testJobContents = `version: 1
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
	execUnit := new(mock.BasePlugin)
	execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
		Name: "foo",
	}, nil)
	pluginRepo := mock.NewPluginRepository(t)
	pluginRepo.On("GetByName", "foo").Return(&models.Plugin{Base: execUnit}, nil)
	adapter := local.NewJobSpecAdapter(pluginRepo)

	jobConfig := local.JobSpec{
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
		Dependencies: []local.JobDependency{
			{
				JobName: "bar",
				Type:    models.JobSpecDependencyTypeIntra.String(),
			},
		},
	}
	window, err := models.NewWindow(1, "d", "0", "24h")
	if err != nil {
		panic(err)
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
			Unit:   &models.Plugin{Base: execUnit},
			Window: window,
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
			Unit:   &models.Plugin{Base: execUnit},
			Window: window,
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
		t.Run("should write the file to ${ROOT}/${name}/job.yaml", func(t *testing.T) {
			appFS := afero.NewMemMapFs()

			repo := local.NewJobSpecRepository(appFS, adapter)
			err := repo.Save(spec)
			assert.Nil(t, err)

			buf, err := afero.ReadFile(appFS, filepath.Join(spec.Name, local.JobSpecFileName))
			assert.Nil(t, err)
			assert.Equal(t, testJobContents, string(buf))

			bufQ, err := afero.ReadFile(appFS, filepath.Join(spec.Name, local.AssetFolderName, "query.sql"))
			assert.Nil(t, err)
			asset, _ := spec.Assets.GetByName("query.sql")
			assert.Equal(t, asset.Value, string(bufQ))
		})
		t.Run("should return error if task is empty", func(t *testing.T) {
			repo := local.NewJobSpecRepository(nil, adapter)
			err := repo.SaveAt(models.JobSpec{Name: "foo"}, "")
			assert.NotNil(t, err)
		})
		t.Run("should return error if name is empty", func(t *testing.T) {
			repo := local.NewJobSpecRepository(nil, adapter)
			err := repo.Save(models.JobSpec{Task: models.JobSpecTask{
				Unit: &models.Plugin{Base: execUnit},
			}})
			assert.NotNil(t, err)
		})
		t.Run("should return error if notify on is unknown", func(t *testing.T) {
			repo := local.NewJobSpecRepository(nil, adapter)
			testSpec := spec2
			testSpec.Behavior.Notify = append(testSpec.Behavior.Notify, models.JobSpecNotifier{
				On: "invalid",
			})
			err := repo.SaveAt(testSpec, "")
			assert.NotNil(t, err)
		})
		t.Run("should update the file with hooks in the same spec ${ROOT}/${name}.yaml", func(t *testing.T) {
			appFS := afero.NewMemMapFs()

			repo := local.NewJobSpecRepository(appFS, adapter)
			err := repo.Save(spec)
			assert.Nil(t, err)
			buf, err := afero.ReadFile(appFS, filepath.Join(spec.Name, local.JobSpecFileName))
			assert.Nil(t, err)
			assert.Equal(t, testJobContents, string(buf))

			bufQ, err := afero.ReadFile(appFS, filepath.Join(spec.Name, local.AssetFolderName, "query.sql"))
			assert.Nil(t, err)
			asset, _ := spec.Assets.GetByName("query.sql")
			assert.Equal(t, asset.Value, string(bufQ))

			// update the spec.
			hookName := "g-hook"
			hookUnit1 := new(mock.BasePlugin)
			hookUnit1.On("PluginInfo").Return(&models.PluginInfoResponse{
				Name:       hookName,
				PluginType: models.PluginTypeHook,
			}, nil)
			pluginsRepo := mock.NewPluginRepository(t)
			pluginsRepo.On("GetByName", "foo").Return(&models.Plugin{Base: execUnit}, nil)
			adapterNew := local.NewJobSpecAdapter(pluginsRepo)

			specCopy := spec
			specCopy.Hooks = []models.JobSpecHook{
				{Config: models.JobSpecConfigs{
					{
						Name:  "key",
						Value: "value",
					},
				}, Unit: &models.Plugin{Base: hookUnit1}},
			}

			repoNew := local.NewJobSpecRepository(appFS, adapterNew)
			err = repoNew.Save(specCopy)
			assert.Nil(t, err)
			testJobContentsNew := strings.ReplaceAll(testJobContents, "hooks: []\n",
				"hooks:\n- name: g-hook\n  config:\n    key: value\n")
			buf, err = afero.ReadFile(appFS, filepath.Join(spec.Name, local.JobSpecFileName))
			assert.Nil(t, err)
			assert.Equal(t, testJobContentsNew, string(buf))
		})
	})

	t.Run("GetByName", func(t *testing.T) {
		t.Run("should open the file and parse its contents", func(t *testing.T) {
			// create test files and directories
			appFS := afero.NewMemMapFs()
			appFS.MkdirAll(spec.Name, 0o755)
			afero.WriteFile(appFS, filepath.Join(spec.Name, local.JobSpecFileName), []byte(testJobContents), 0o644)
			appFS.MkdirAll(filepath.Join(spec.Name, local.AssetFolderName), 0o755)
			afero.WriteFile(appFS, filepath.Join(spec.Name, local.AssetFolderName, "query.sql"), []byte(jobConfig.Asset["query.sql"]), 0o644)

			repo := local.NewJobSpecRepository(appFS, adapter)
			returnedSpec, err := repo.GetByName(spec.Name)
			assert.Nil(t, err)
			assert.Equal(t, spec2, returnedSpec)
		})
		t.Run("should read the spec and inherit configuration from direct parent directory", func(t *testing.T) {
			thisYamlContent := `version: 1
owner: optimus
behavior:
  depends_on_past: false
  catch_up: true
task:
  config:
    project: proj1
  window:
    size: 24h
    offset: "0"
    truncate_to: d
dependencies:
- job: bar
hooks: []`
			testJobContentsLocal := `name: test
owner: optimus
schedule:
  start_date: "2020-12-02"
  interval: '@daily'
task:
  name: foo
  config:
    table: tab1`
			// create test files and directories
			// ./this.yaml
			// ./spec/
			// ./spec/job.yaml
			// ./spec/asset/query.sql
			appFS := afero.NewMemMapFs()
			afero.WriteFile(appFS, local.JobSpecParentName, []byte(thisYamlContent), 0o644)
			appFS.MkdirAll(spec.Name, 0o755)
			afero.WriteFile(appFS, filepath.Join(spec.Name, local.JobSpecFileName), []byte(testJobContentsLocal), 0o644)
			appFS.MkdirAll(filepath.Join(spec.Name, local.AssetFolderName), 0o755)
			afero.WriteFile(appFS, filepath.Join(spec.Name, local.AssetFolderName, "query.sql"), []byte(jobConfig.Asset["query.sql"]), 0o644)

			repo := local.NewJobSpecRepository(appFS, adapter)
			returnedSpec, err := repo.GetByName(spec.Name)
			assert.Nil(t, err)
			expectedSpec := spec2
			expectedSpec.Task.Config = append(expectedSpec.Task.Config, models.JobSpecConfigItem{
				Name:  "project",
				Value: "proj1",
			})
			assert.Equal(t, expectedSpec, returnedSpec)
		})
		t.Run("should read the spec and inherit configuration from all of its parent directories", func(t *testing.T) {
			thisYamlContentRoot := `version: 1
owner: optimus
behavior:
  depends_on_past: false
  catch_up: false
task:
  config:
    project: proj1
  window:
    size: 24h
    offset: "0"
    truncate_to: d
dependencies:
- job: bar
hooks: []`
			thisYamlContentSubFolder := `description: super secret job
behavior:
  catch_up: true
`
			testJobContentsLocal := `name: test
schedule:
  start_date: "2020-12-02"
  interval: '@daily'
task:
  name: foo
  config:
    table: tab1`
			// create test files and directories
			// ./this.yaml
			// ./secret_jobs/this.yaml
			// ./secret_jobs/spec/
			// ./secret_jobs/spec/job.yaml
			// ./secret_jobs/spec/asset/query.sql
			appFS := afero.NewMemMapFs()
			afero.WriteFile(appFS, local.JobSpecParentName, []byte(thisYamlContentRoot), 0o644)
			appFS.MkdirAll("secret_jobs", 0o755)

			appFS.MkdirAll(filepath.Join("secret_jobs", spec.Name), 0o755)
			afero.WriteFile(appFS, filepath.Join("secret_jobs", local.JobSpecParentName), []byte(thisYamlContentSubFolder), 0o644)

			afero.WriteFile(appFS, filepath.Join("secret_jobs", spec.Name, local.JobSpecFileName), []byte(testJobContentsLocal), 0o644)
			appFS.MkdirAll(filepath.Join("secret_jobs", spec.Name, local.AssetFolderName), 0o755)
			afero.WriteFile(appFS, filepath.Join("secret_jobs", spec.Name, local.AssetFolderName, "query.sql"), []byte(jobConfig.Asset["query.sql"]), 0o644)

			repo := local.NewJobSpecRepository(appFS, adapter)
			returnedSpec, err := repo.GetByName(spec.Name)
			assert.Nil(t, err)
			expectedSpec := spec2
			expectedSpec.Description = "super secret job"
			expectedSpec.Task.Config = append(expectedSpec.Task.Config, models.JobSpecConfigItem{
				Name:  "project",
				Value: "proj1",
			})
			assert.Equal(t, expectedSpec, returnedSpec)
		})
		t.Run("should use cache if file is requested more than once", func(t *testing.T) {
			// create test files and directories
			appFS := afero.NewMemMapFs()
			appFS.MkdirAll(spec.Name, 0o755)
			afero.WriteFile(appFS, filepath.Join(spec.Name, local.JobSpecFileName), []byte(testJobContents), 0o644)
			appFS.MkdirAll(filepath.Join(spec.Name, local.AssetFolderName), 0o755)
			afero.WriteFile(appFS, filepath.Join(spec.Name, local.AssetFolderName, "query.sql"), []byte(jobConfig.Asset["query.sql"]), 0o644)

			repo := local.NewJobSpecRepository(appFS, adapter)
			returnedSpec, err := repo.GetByName(spec.Name)
			assert.Nil(t, err)
			assert.Equal(t, spec2, returnedSpec)

			// delete all specs
			assert.Nil(t, appFS.RemoveAll(spec.Name))

			returnedSpecAgain, err := repo.GetByName(spec.Name)
			assert.Nil(t, err)
			assert.Equal(t, spec2, returnedSpecAgain)
		})
		t.Run("should return ErrNoSuchSpec in case no job folder exist", func(t *testing.T) {
			// create test files and directories
			appFS := afero.NewMemMapFs()

			repo := local.NewJobSpecRepository(appFS, adapter)
			_, err := repo.GetByName(spec.Name)
			assert.Equal(t, models.ErrNoSuchSpec, err)
		})
		t.Run("should return ErrNoSuchSpec in case the job folder exist but no job file exist", func(t *testing.T) {
			appFS := afero.NewMemMapFs()
			appFS.MkdirAll(spec.Name, 0o755)

			repo := local.NewJobSpecRepository(appFS, adapter)
			_, err := repo.GetByName(spec.Name)
			assert.Equal(t, models.ErrNoSuchSpec, err)
		})
		t.Run("should return an error if jobName is empty", func(t *testing.T) {
			repo := local.NewJobSpecRepository(afero.NewMemMapFs(), nil)
			_, err := repo.GetByName("")
			assert.NotNil(t, err)
		})
		t.Run("should return error if yaml source is incorrect and failed to validate", func(t *testing.T) {
			// create test files and directories
			appFS := afero.NewMemMapFs()
			appFS.MkdirAll(spec.Name, 0o755)
			afero.WriteFile(appFS, filepath.Join(spec.Name, local.JobSpecFileName), []byte("name:a"), 0o644)

			repo := local.NewJobSpecRepository(appFS, adapter)
			_, err := repo.GetByName(spec.Name)
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
   truncate_to: d`,
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
hooks: []`,
		}
		window, err := models.NewWindow(1, "d", "0", "24h")
		if err != nil {
			panic(err)
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
					Unit:   &models.Plugin{Base: execUnit},
					Config: models.JobSpecConfigs{},
					Window: window,
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
					Unit:   &models.Plugin{Base: execUnit},
					Config: models.JobSpecConfigs{},
					Window: window,
				},
				Dependencies: map[string]models.JobSpecDependency{},
				Assets:       models.JobAssets{},
				Labels:       map[string]string{},
			},
		}

		t.Run("should read and parse all files under ${ROOT}", func(t *testing.T) {
			// create test files and directories
			appFS := afero.NewMemMapFs()

			for idx, jobspec := range jobspecs {
				appFS.MkdirAll(jobspec.Name, 0o755)
				afero.WriteFile(appFS, filepath.Join(jobspec.Name, local.JobSpecFileName), []byte(content[idx]), 0o644)
				appFS.MkdirAll(filepath.Join(jobspec.Name, local.AssetFolderName), 0o755)
			}

			repo := local.NewJobSpecRepository(appFS, adapter)
			result, err := repo.GetAll()
			assert.Nil(t, err)
			assert.Equal(t, len(jobspecs), len(result))

			// sort result
			sort.Slice(result, func(i, j int) bool { return result[i].Name > result[j].Name })
			assert.Equal(t, jobspecs, result)
		})
		t.Run("should return ErrNoSpecsFound if the root directory does not exist", func(t *testing.T) {
			repo := local.NewJobSpecRepository(afero.NewMemMapFs(), adapter)
			_, err := repo.GetAll()
			assert.Equal(t, models.ErrNoJobs, err)
		})
		t.Run("should return ErrNoSpecsFound if the root directory has no files", func(t *testing.T) {
			appFS := afero.NewMemMapFs()
			appFS.MkdirAll("test", 0o755)

			repo := local.NewJobSpecRepository(appFS, adapter)
			_, err := repo.GetAll()
			assert.Equal(t, models.ErrNoJobs, err)
		})
		t.Run("should use cache to return specs if called more than once", func(t *testing.T) {
			appFS := afero.NewMemMapFs()

			for idx, jobspec := range jobspecs {
				appFS.MkdirAll(jobspec.Name, 0o755)
				afero.WriteFile(appFS, filepath.Join(jobspec.Name, local.JobSpecFileName), []byte(content[idx]), 0o644)
				appFS.MkdirAll(filepath.Join(jobspec.Name, local.AssetFolderName), 0o755)
			}

			repo := local.NewJobSpecRepository(appFS, adapter)
			result, err := repo.GetAll()
			assert.Nil(t, err)
			sort.Slice(result, func(i, j int) bool { return result[i].Name > result[j].Name })
			assert.Equal(t, jobspecs, result)

			// clear fs
			assert.Nil(t, appFS.RemoveAll("."))

			resultAgain, err := repo.GetAll()
			assert.Nil(t, err)
			assert.Equal(t, len(result), len(resultAgain))
		})
	})
}
