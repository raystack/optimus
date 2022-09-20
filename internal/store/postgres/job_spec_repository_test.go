//go:build !unit_test
// +build !unit_test

package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/odpf/optimus/internal/store/postgres"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

type JobSpecRepositoryTestSuite struct {
	suite.Suite

	db *gorm.DB
}

func (j *JobSpecRepositoryTestSuite) SetupTest() {
	// TODO: check if we can refactor this to avoid using public db connection variable
	migrateDB()
	j.db = optimusDB
	j.db.Logger = logger.Default.LogMode(logger.Info)
}

func (j *JobSpecRepositoryTestSuite) TearDownSuite() {
	db, err := j.db.DB()
	if err != nil {
		panic(err)
	}
	if err := db.Close(); err != nil {
		panic(err)
	}
	migrateDB()
}

func (j *JobSpecRepositoryTestSuite) TestGetAllByProjectName() {
	project0 := j.getDummyProject("project_test0")
	namespace0 := j.getDummyNamespace("namespace_test0", project0)
	job0 := j.getDummyJob("job_test0", "destination_test0", namespace0)
	job1 := j.getDummyJob("job_test1", "destination_test1", namespace0)

	project1 := j.getDummyProject("project_test1")
	namespace1 := j.getDummyNamespace("namespace_test1", project1)
	job2 := j.getDummyJob("job_test2", "destination_test2", namespace1)

	insertRecords(j.db, []*postgres.Project{project0, project1})
	insertRecords(j.db, []*postgres.Namespace{namespace0, namespace1})
	insertRecords(j.db, []*postgres.Job{job0, job1, job2})

	j.Run("should return all jobs within a project and nil", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		projectName := project0.Name

		actualJobSpecs, actualError := repository.GetAllByProjectName(ctx, projectName)

		j.NoError(actualError)
		j.Len(actualJobSpecs, 2)
		j.Equal(job0.Name, actualJobSpecs[0].Name)
		j.Equal(job0.Project.Name, actualJobSpecs[0].NamespaceSpec.ProjectSpec.Name)
		j.Equal(job1.Name, actualJobSpecs[1].Name)
		j.Equal(job1.Project.Name, actualJobSpecs[1].NamespaceSpec.ProjectSpec.Name)
	})
}

func (j *JobSpecRepositoryTestSuite) TestGetAllByProjectNameAndNamespaceName() {
	project := j.getDummyProject("project_test")
	namespace0 := j.getDummyNamespace("namespace_test0", project)
	namespace1 := j.getDummyNamespace("namespace_test1", project)
	job0 := j.getDummyJob("job_test0", "destination_test0", namespace0)
	job1 := j.getDummyJob("job_test1", "destination_test1", namespace0)
	job2 := j.getDummyJob("job_test2", "destination_test2", namespace1)

	insertRecords(j.db, []*postgres.Project{project})
	insertRecords(j.db, []*postgres.Namespace{namespace0, namespace1})
	insertRecords(j.db, []*postgres.Job{job0, job1, job2})

	j.Run("should return all jobs within a project with the specified namespace and nil", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		projectName := project.Name
		namespaceName := namespace0.Name

		actualJobSpecs, actualError := repository.GetAllByProjectNameAndNamespaceName(ctx, projectName, namespaceName)

		j.NoError(actualError)
		j.Len(actualJobSpecs, 2)
		j.Equal(job0.Name, actualJobSpecs[0].Name)
		j.Equal(job0.Namespace.Name, actualJobSpecs[0].NamespaceSpec.Name)
		j.Equal(job1.Name, actualJobSpecs[1].Name)
		j.Equal(job1.Namespace.Name, actualJobSpecs[1].NamespaceSpec.Name)
	})
}

func (j *JobSpecRepositoryTestSuite) TestGetByNameAndProjectName() {
	project := j.getDummyProject("project_test")
	namespace := j.getDummyNamespace("namespace_test", project)
	job := j.getDummyJob("job_test", "destination_test", namespace)

	insertRecords(j.db, []*postgres.Project{project})
	insertRecords(j.db, []*postgres.Namespace{namespace})
	insertRecords(j.db, []*postgres.Job{job})

	j.Run("should return empty and error if no job is found", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		jobName := "unknown_job"
		projectName := project.Name

		actualJobSpec, actualError := repository.GetByNameAndProjectName(ctx, jobName, projectName)

		j.Error(actualError)
		j.Empty(actualJobSpec)
	})

	j.Run("should return job spec and nil if no error is encountered", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		jobName := job.Name
		projectName := project.Name

		actualJobSpec, actualError := repository.GetByNameAndProjectName(ctx, jobName, projectName)

		j.NoError(actualError)
		j.Equal(jobName, actualJobSpec.Name)
	})
}

func (j *JobSpecRepositoryTestSuite) TestGetByResourceDestinationURN() {
	project := j.getDummyProject("project_test")
	namespace := j.getDummyNamespace("namespace_test", project)
	job := j.getDummyJob("job_test", "destination_test", namespace)

	insertRecords(j.db, []*postgres.Project{project})
	insertRecords(j.db, []*postgres.Namespace{namespace})
	insertRecords(j.db, []*postgres.Job{job})

	j.Run("should return empty and error if error is encountered", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		destination := "unknown_destination"

		actualJobSpec, actualError := repository.GetByResourceDestinationURN(ctx, destination)

		j.Error(actualError)
		j.Empty(actualJobSpec)
	})

	j.Run("should return job and nil if no error encountered", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		destination := job.Destination

		actualJobSpec, actualError := repository.GetByResourceDestinationURN(ctx, destination)

		j.NoError(actualError)
		j.Equal(job.Name, actualJobSpec.Name)
	})
}

func (j *JobSpecRepositoryTestSuite) TestGetDependentJobs() {
	project := j.getDummyProject("project_test")
	namespace := j.getDummyNamespace("namespace_test", project)
	job0 := j.getDummyJob("job_test0", "destination_test0", namespace)
	job1 := j.getDummyJob("job_test1", "destination_test1", namespace)
	job2 := j.getDummyJob("job_test2", "destination_test2", namespace)
	job3 := j.getDummyJob("job_test3", "destination_test3", namespace)

	job1Dependencies, _ := json.Marshal(map[string]models.JobSpecDependency{
		job0.Name: {Type: models.JobSpecDependencyTypeIntra},
	})
	job1.Dependencies = job1Dependencies
	job2Dependencies, _ := json.Marshal(map[string]models.JobSpecDependency{
		fmt.Sprintf("%s/%s", project.Name, job0.Name): {Type: models.JobSpecDependencyTypeIntra},
	})
	job2.Dependencies = job2Dependencies

	insertRecords(j.db, []*postgres.Project{project})
	insertRecords(j.db, []*postgres.Namespace{namespace})
	insertRecords(j.db, []*postgres.Job{job0, job1, job2, job3})

	job3JobSource := &postgres.JobSource{JobID: job3.ID, ResourceURN: job0.Destination, ProjectID: project.ID}
	insertRecords(j.db, []*postgres.JobSource{job3JobSource})

	j.Run("should return dependent jobs and nil if no error is encountered", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		jobName := job0.Name
		resourceDestinationURN := job0.Destination
		projectName := project.Name

		actualJobSpecs, actualError := repository.GetDependentJobs(ctx, jobName, resourceDestinationURN, projectName)

		j.NoError(actualError)
		j.Len(actualJobSpecs, 3)
		j.Equal(job1.Name, actualJobSpecs[1].Name)
		j.Equal(job2.Name, actualJobSpecs[2].Name)
		j.Equal(job3.Name, actualJobSpecs[0].Name)
	})
}

func (j *JobSpecRepositoryTestSuite) TestGetInferredDependenciesPerJobID() {
	project := j.getDummyProject("project_test")
	namespace := j.getDummyNamespace("namespace_test", project)
	job0 := j.getDummyJob("job_test0", "destination_test0", namespace)
	job1 := j.getDummyJob("job_test1", "destination_test1", namespace)

	insertRecords(j.db, []*postgres.Project{project})
	insertRecords(j.db, []*postgres.Namespace{namespace})
	insertRecords(j.db, []*postgres.Job{job0, job1})

	job0JobSource := &postgres.JobSource{JobID: job0.ID, ResourceURN: job1.Destination, ProjectID: project.ID}
	insertRecords(j.db, []*postgres.JobSource{job0JobSource})

	j.Run("should return inferred dependencies per job id within a project and nil if no error is encountered", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		projectName := project.Name

		actualInferredDependenciesPerJobID, actualError := repository.GetInferredDependenciesPerJobID(ctx, projectName)

		j.NoError(actualError)
		j.Len(actualInferredDependenciesPerJobID, 1)
		j.Len(actualInferredDependenciesPerJobID[job0.ID], 1)
		j.Equal(job1.Name, actualInferredDependenciesPerJobID[job0.ID][0].Name)
	})
}

func (j *JobSpecRepositoryTestSuite) TestGetStaticDependenciesPerJobID() {
	project := j.getDummyProject("project_test")
	namespace := j.getDummyNamespace("namespace_test", project)
	job0 := j.getDummyJob("job_test0", "destination_test0", namespace)
	job1 := j.getDummyJob("job_test1", "destination_test1", namespace)

	job0Dependencies, _ := json.Marshal(map[string]models.JobSpecDependency{
		job1.Name: {Type: models.JobSpecDependencyTypeIntra},
	})
	job0.Dependencies = job0Dependencies

	insertRecords(j.db, []*postgres.Project{project})
	insertRecords(j.db, []*postgres.Namespace{namespace})
	insertRecords(j.db, []*postgres.Job{job0, job1})

	j.Run("should return static dependencies per job id within a project and nil if no error is encountered", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		projectName := project.Name

		actualInferredDependenciesPerJobID, actualError := repository.GetStaticDependenciesPerJobID(ctx, projectName)

		j.NoError(actualError)
		j.Len(actualInferredDependenciesPerJobID, 1)
		j.Len(actualInferredDependenciesPerJobID[job0.ID], 1)
		j.Equal(job1.Name, actualInferredDependenciesPerJobID[job0.ID][0].Name)
	})
}

func (j *JobSpecRepositoryTestSuite) TestSave() {
	project := j.getDummyProject("project_test")
	namespace := j.getDummyNamespace("namespace_test", project)
	existingJob := j.getDummyJob("job_test", "destination_test", namespace)

	insertRecords(j.db, []*postgres.Project{project})
	insertRecords(j.db, []*postgres.Namespace{namespace})
	insertRecords(j.db, []*postgres.Job{existingJob})

	j.Run("should return error if error encountered when saving", func() {
		pluginRepository := mock.NewPluginRepository(j.T())

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		inputJobSpec := models.JobSpec{
			Name: existingJob.Name,
			NamespaceSpec: models.NamespaceSpec{
				ID:   namespace.ID,
				Name: namespace.Name,
				ProjectSpec: models.ProjectSpec{
					ID:   models.ProjectID(project.ID),
					Name: project.Name,
				},
			},
		}

		actualError := repository.Save(ctx, inputJobSpec)

		j.Error(actualError)
	})

	j.Run("should insert job spec if not exist and return nil if no error is encountered", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		execUnit := &mock.BasePlugin{}
		execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name:       "task",
			PluginType: models.PluginTypeTask,
		}, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		inputJobSpec := models.JobSpec{
			Name: "job_test2",
			NamespaceSpec: models.NamespaceSpec{
				ID:   namespace.ID,
				Name: namespace.Name,
				ProjectSpec: models.ProjectSpec{
					ID:   models.ProjectID(project.ID),
					Name: project.Name,
				},
			},
			Task: models.JobSpecTask{
				Unit: &models.Plugin{Base: execUnit},
			},
		}

		actualError := repository.Save(ctx, inputJobSpec)
		storedJobs := readStoredRecordsByFilter[*postgres.Job](j.db, map[string]interface{}{
			"name": inputJobSpec.Name,
		})

		j.NoError(actualError)
		j.Equal(inputJobSpec.Name, storedJobs[0].Name)
	})

	j.Run("should update existing job spec and return nil if no error is encountered", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		execUnit := &mock.BasePlugin{}
		execUnit.On("PluginInfo").Return(&models.PluginInfoResponse{
			Name:       "task",
			PluginType: models.PluginTypeTask,
		}, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		inputJobSpec := models.JobSpec{
			Name:        existingJob.Name,
			Description: "updated description",
			NamespaceSpec: models.NamespaceSpec{
				ID:   namespace.ID,
				Name: namespace.Name,
				ProjectSpec: models.ProjectSpec{
					ID:   models.ProjectID(project.ID),
					Name: project.Name,
				},
			},
			Task: models.JobSpecTask{
				Unit: &models.Plugin{Base: execUnit},
			},
		}

		actualError := repository.Save(ctx, inputJobSpec)
		storedJobs := readStoredRecordsByFilter[*postgres.Job](j.db, map[string]interface{}{
			"name": inputJobSpec.Name,
		})

		j.NoError(actualError)
		j.Equal(inputJobSpec.Description, storedJobs[0].Description)
	})
}

func (j *JobSpecRepositoryTestSuite) TestDeleteByID() {
	project := j.getDummyProject("project_test")
	namespace := j.getDummyNamespace("namespace_test", project)
	existingJob := j.getDummyJob("job_test", "destination_test", namespace)

	insertRecords(j.db, []*postgres.Project{project})
	insertRecords(j.db, []*postgres.Namespace{namespace})
	insertRecords(j.db, []*postgres.Job{existingJob})

	existingJobSource := &postgres.JobSource{JobID: existingJob.ID, ResourceURN: existingJob.Destination, ProjectID: project.ID}
	insertRecords(j.db, []*postgres.JobSource{existingJobSource})

	j.Run("should return nil if the targeted id does not exist", func() {
		pluginRepository := mock.NewPluginRepository(j.T())

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		inputID := uuid.New()

		actualError := repository.DeleteByID(ctx, inputID)
		storedJobs := readStoredRecords[*postgres.Job](j.db)
		storedJobSources := readStoredRecords[*postgres.JobSource](j.db)

		j.NoError(actualError)
		j.Len(storedJobs, 1)
		j.Len(storedJobSources, 1)
	})

	j.Run("should delete job and job sources specified by id and return nil if no error is encountered during delete", func() {
		pluginRepository := mock.NewPluginRepository(j.T())

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		inputID := existingJob.ID

		actualError := repository.DeleteByID(ctx, inputID)
		storedJobs := readStoredRecords[*postgres.Job](j.db)
		storedJobSources := readStoredRecords[*postgres.JobSource](j.db)

		j.NoError(actualError)
		j.Len(storedJobs, 0)
		j.Len(storedJobSources, 0)
	})
}

func (*JobSpecRepositoryTestSuite) getDummyJob(name, destination string, namespace *postgres.Namespace) *postgres.Job {
	dependencies, _ := json.Marshal(map[string]models.JobSpecDependency{})
	taskConfigs, _ := json.Marshal(models.JobSpecConfigs{})
	assets, _ := json.Marshal([]postgres.JobAsset{})
	hooks, _ := json.Marshal([]postgres.JobHook{})
	var interval string
	return &postgres.Job{
		ID:           uuid.New(),
		Version:      1,
		Name:         name,
		NamespaceID:  namespace.ID,
		Namespace:    *namespace,
		ProjectID:    namespace.ProjectID,
		Project:      namespace.Project,
		Destination:  destination,
		Dependencies: dependencies,
		TaskConfig:   taskConfigs,
		Assets:       assets,
		Hooks:        hooks,
		Interval:     &interval,
	}
}

func (*JobSpecRepositoryTestSuite) getDummyNamespace(name string, project *postgres.Project) *postgres.Namespace {
	config, _ := json.Marshal(map[string]string{})
	return &postgres.Namespace{
		ID:        uuid.New(),
		Name:      name,
		Project:   *project,
		ProjectID: project.ID,
		Config:    config,
	}
}

func (*JobSpecRepositoryTestSuite) getDummyProject(name string) *postgres.Project {
	config := map[string]string{
		"bucket": "gs://some_folder",
	}
	configJSON, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return &postgres.Project{
		ID:     uuid.New(),
		Name:   name,
		Config: configJSON,
	}
}

func TestNewJobSpecRepository(t *testing.T) {
	plugin := mock.NewPluginRepository(t)

	t.Run("should return nil and error if db client is nil", func(t *testing.T) {
		var db *gorm.DB
		adapter := postgres.NewAdapter(plugin)

		actualRepository, actualError := postgres.NewJobSpecRepository(db, adapter)

		assert.Nil(t, actualRepository)
		assert.Error(t, actualError)
	})

	t.Run("should return nil and error if adapter is nil", func(t *testing.T) {
		db := &gorm.DB{}
		var adapter *postgres.JobSpecAdapter

		actualRepository, actualError := postgres.NewJobSpecRepository(db, adapter)

		assert.Nil(t, actualRepository)
		assert.Error(t, actualError)
	})

	t.Run("should return repository and nil if no error is encountered", func(t *testing.T) {
		db := &gorm.DB{}
		adapter := postgres.NewAdapter(plugin)

		actualRepository, actualError := postgres.NewJobSpecRepository(db, adapter)

		assert.NotNil(t, actualRepository)
		assert.NoError(t, actualError)
	})
}

func TestJobSpecRepository(t *testing.T) {
	suite.Run(t, &JobSpecRepositoryTestSuite{})
}
