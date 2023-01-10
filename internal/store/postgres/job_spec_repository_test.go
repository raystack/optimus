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
	projectTarget := j.getDummyProject("project_target")
	namespaceTarget := j.getDummyNamespace("namespace_target", projectTarget)
	job1Target := j.getDummyJob("job1_target", "destination1_target", namespaceTarget)
	job2Target := j.getDummyJob("job2_target", "destination2_target", namespaceTarget)

	projectToIgnore := j.getDummyProject("project_to_ignore")
	namespaceToIgnore := j.getDummyNamespace("namespace_to_ignore", projectToIgnore)
	jobToIgnore := j.getDummyJob("job_to_ignore", "destination_to_ignore", namespaceToIgnore)

	insertRecords(j.db, []*postgres.Project{projectTarget, projectToIgnore})
	insertRecords(j.db, []*postgres.Namespace{namespaceTarget, namespaceToIgnore})
	insertRecords(j.db, []*postgres.Job{job1Target, job2Target, jobToIgnore})

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
		projectName := projectTarget.Name

		actualJobSpecs, actualError := repository.GetAllByProjectName(ctx, projectName, false)
		j.NoError(actualError)
		j.Len(actualJobSpecs, 2)
		j.Equal(job1Target.Name, actualJobSpecs[0].Name)
		j.Equal(job1Target.Project.Name, actualJobSpecs[0].NamespaceSpec.ProjectSpec.Name)
		j.Equal(job2Target.Name, actualJobSpecs[1].Name)
		j.Equal(job2Target.Project.Name, actualJobSpecs[1].NamespaceSpec.ProjectSpec.Name)
	})

	j.Run("should return all jobs within a project including the deleted and nil", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		projectName := projectTarget.Name

		jobToDelete, err := repository.GetByNameAndProjectName(ctx, job2Target.Name, projectName, false)
		j.NoError(err)

		err = repository.DeleteByID(ctx, jobToDelete.ID)
		j.NoError(err)

		actualJobSpecs, actualError := repository.GetAllByProjectName(ctx, projectName, true)
		j.NoError(actualError)
		j.Len(actualJobSpecs, 2)
		j.Equal(job1Target.Name, actualJobSpecs[0].Name)
		j.Equal(job1Target.Project.Name, actualJobSpecs[0].NamespaceSpec.ProjectSpec.Name)
		j.Equal(job2Target.Name, actualJobSpecs[1].Name)
		j.Equal(job2Target.Project.Name, actualJobSpecs[1].NamespaceSpec.ProjectSpec.Name)
	})

	j.Run("should return all jobs within a project excluding the deleted and nil", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		projectName := projectTarget.Name

		jobToDelete, err := repository.GetByNameAndProjectName(ctx, job2Target.Name, projectName, false)
		j.NoError(err)

		err = repository.DeleteByID(ctx, jobToDelete.ID)
		j.NoError(err)

		actualJobSpecs, actualError := repository.GetAllByProjectName(ctx, projectName, false)
		j.NoError(actualError)
		j.Len(actualJobSpecs, 1)
		j.Equal(job1Target.Name, actualJobSpecs[0].Name)
		j.Equal(job1Target.Project.Name, actualJobSpecs[0].NamespaceSpec.ProjectSpec.Name)
	})
}

func (j *JobSpecRepositoryTestSuite) TestGetAllByProjectNameAndNamespaceName() {
	projectTarget := j.getDummyProject("project_target")
	namespaceTarget := j.getDummyNamespace("namespace_target", projectTarget)
	namespaceToIgnore := j.getDummyNamespace("namespace_to_ignore", projectTarget)
	job1Target := j.getDummyJob("job1_target", "destination1_target", namespaceTarget)
	job2Target := j.getDummyJob("job2_target", "destination2_target", namespaceTarget)
	jobToIgnore := j.getDummyJob("job_to_ignore", "destination_to_ignore", namespaceToIgnore)

	insertRecords(j.db, []*postgres.Project{projectTarget})
	insertRecords(j.db, []*postgres.Namespace{namespaceTarget, namespaceToIgnore})
	insertRecords(j.db, []*postgres.Job{job1Target, job2Target, jobToIgnore})

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
		projectName := projectTarget.Name
		namespaceName := namespaceTarget.Name

		actualJobSpecs, actualError := repository.GetAllByProjectNameAndNamespaceName(ctx, projectName, namespaceName, false)
		j.NoError(actualError)
		j.Len(actualJobSpecs, 2)
		j.Equal(job1Target.Name, actualJobSpecs[0].Name)
		j.Equal(job1Target.Namespace.Name, actualJobSpecs[0].NamespaceSpec.Name)
		j.Equal(job2Target.Name, actualJobSpecs[1].Name)
		j.Equal(job2Target.Namespace.Name, actualJobSpecs[1].NamespaceSpec.Name)
	})

	j.Run("should return all jobs including deleted jobs within a project with the specified namespace and nil", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		projectName := projectTarget.Name
		namespaceName := namespaceTarget.Name

		jobToDelete, err := repository.GetByNameAndProjectName(ctx, job2Target.Name, projectName, false)
		j.NoError(err)

		err = repository.DeleteByID(ctx, jobToDelete.ID)
		j.NoError(err)

		actualJobSpecs, actualError := repository.GetAllByProjectNameAndNamespaceName(ctx, projectName, namespaceName, true)
		j.NoError(actualError)
		j.Len(actualJobSpecs, 2)
		j.Equal(job1Target.Name, actualJobSpecs[0].Name)
		j.Equal(job1Target.Namespace.Name, actualJobSpecs[0].NamespaceSpec.Name)
		j.Equal(job2Target.Name, actualJobSpecs[1].Name)
		j.Equal(job2Target.Namespace.Name, actualJobSpecs[1].NamespaceSpec.Name)
	})

	j.Run("should return all jobs excluding deleted jobs within a project with the specified namespace and nil", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		projectName := projectTarget.Name
		namespaceName := namespaceTarget.Name

		jobToDelete, err := repository.GetByNameAndProjectName(ctx, job2Target.Name, projectName, false)
		j.NoError(err)

		err = repository.DeleteByID(ctx, jobToDelete.ID)
		j.NoError(err)

		actualJobSpecs, actualError := repository.GetAllByProjectNameAndNamespaceName(ctx, projectName, namespaceName, false)
		j.NoError(actualError)
		j.Len(actualJobSpecs, 1)
		j.Equal(job1Target.Name, actualJobSpecs[0].Name)
		j.Equal(job1Target.Namespace.Name, actualJobSpecs[0].NamespaceSpec.Name)
	})
}

func (j *JobSpecRepositoryTestSuite) TestGetByNameAndProjectName() {
	storedProject := j.getDummyProject("project_test")
	storedNamespace := j.getDummyNamespace("namespace_test", storedProject)
	storedJob := j.getDummyJob("job_test", "destination_test", storedNamespace)

	insertRecords(j.db, []*postgres.Project{storedProject})
	insertRecords(j.db, []*postgres.Namespace{storedNamespace})
	insertRecords(j.db, []*postgres.Job{storedJob})

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
		projectName := storedProject.Name

		actualJobSpec, actualError := repository.GetByNameAndProjectName(ctx, jobName, projectName, false)
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
		jobName := storedJob.Name
		projectName := storedProject.Name

		actualJobSpec, actualError := repository.GetByNameAndProjectName(ctx, jobName, projectName, false)
		j.NoError(actualError)
		j.Equal(jobName, actualJobSpec.Name)
	})

	j.Run("should return deleted job spec and nil if no error is encountered and include deleted is true", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		jobName := storedJob.Name
		projectName := storedProject.Name

		jobToDelete, err := repository.GetByNameAndProjectName(ctx, jobName, storedProject.Name, false)
		j.NoError(err)

		err = repository.DeleteByID(ctx, jobToDelete.ID)
		j.NoError(err)

		actualJobSpec, actualError := repository.GetByNameAndProjectName(ctx, jobName, projectName, true)
		j.NoError(actualError)
		j.Equal(jobName, actualJobSpec.Name)
	})

	j.Run("should return job spec excluding the deleted and nil if no error is encountered and include deleted is false", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		jobName := storedJob.Name
		projectName := storedProject.Name

		jobToDelete, err := repository.GetByNameAndProjectName(ctx, jobName, storedProject.Name, false)
		j.NoError(err)

		err = repository.DeleteByID(ctx, jobToDelete.ID)
		j.NoError(err)

		actualJobSpec, actualError := repository.GetByNameAndProjectName(ctx, jobName, projectName, false)
		j.Error(actualError)
		j.Empty(actualJobSpec)
	})
}

func (j *JobSpecRepositoryTestSuite) TestGetByResourceDestinationURN() {
	storedProject := j.getDummyProject("project_test")
	storedNamespace := j.getDummyNamespace("namespace_test", storedProject)
	storedJob := j.getDummyJob("job_test", "destination_test", storedNamespace)

	insertRecords(j.db, []*postgres.Project{storedProject})
	insertRecords(j.db, []*postgres.Namespace{storedNamespace})
	insertRecords(j.db, []*postgres.Job{storedJob})

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
		destination := storedJob.Destination

		actualJobSpec, actualError := repository.GetByResourceDestinationURN(ctx, destination, false)
		j.NoError(actualError)
		j.Equal(storedJob.Name, actualJobSpec[0].Name)
	})

	j.Run("should return job including the deleted and nil if no error encountered", func() {
		pluginRepository := mock.NewPluginRepository(j.T())
		pluginRepository.On("GetByName", "").Return(nil, nil)

		db := j.db
		adapter := postgres.NewAdapter(pluginRepository)
		repository, err := postgres.NewJobSpecRepository(db, adapter)
		if err != nil {
			panic(err)
		}

		ctx := context.Background()
		destination := storedJob.Destination

		jobToDelete, err := repository.GetByNameAndProjectName(ctx, storedJob.Name, storedProject.Name, false)
		j.NoError(err)

		err = repository.DeleteByID(ctx, jobToDelete.ID)
		j.NoError(err)

		actualJobSpec, actualError := repository.GetByResourceDestinationURN(ctx, destination, true)
		j.NoError(actualError)
		j.Equal(storedJob.Name, actualJobSpec[0].Name)
	})
}

func (j *JobSpecRepositoryTestSuite) TestGetDependentJobs() {
	projectTarget := j.getDummyProject("project_test")
	namespaceTarget := j.getDummyNamespace("namespace_test", projectTarget)
	jobWithNoDependency := j.getDummyJob("job_with_no_dependency", "destination_with_no_dependency_test", namespaceTarget)
	job1 := j.getDummyJob("job1", "destination1", namespaceTarget)
	job2 := j.getDummyJob("job2", "destination2", namespaceTarget)
	job3 := j.getDummyJob("job3", "destination3", namespaceTarget)

	dependenciesForJob1, _ := json.Marshal(map[string]models.JobSpecDependency{
		jobWithNoDependency.Name: {Type: models.JobSpecDependencyTypeIntra},
	})
	job1.Dependencies = dependenciesForJob1
	dependenciesForJob2, _ := json.Marshal(map[string]models.JobSpecDependency{
		fmt.Sprintf("%s/%s", projectTarget.Name, jobWithNoDependency.Name): {Type: models.JobSpecDependencyTypeIntra},
	})
	job2.Dependencies = dependenciesForJob2

	insertRecords(j.db, []*postgres.Project{projectTarget})
	insertRecords(j.db, []*postgres.Namespace{namespaceTarget})
	insertRecords(j.db, []*postgres.Job{jobWithNoDependency, job1, job2, job3})

	jobSourceForJob3 := &postgres.JobSource{JobID: job3.ID, ResourceURN: jobWithNoDependency.Destination, ProjectID: projectTarget.ID}
	insertRecords(j.db, []*postgres.JobSource{jobSourceForJob3})

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
		jobName := jobWithNoDependency.Name
		resourceDestinationURN := jobWithNoDependency.Destination
		projectName := projectTarget.Name

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
	jobWithDependency := j.getDummyJob("job_with_dependency", "destination1", namespace)
	jobWithNoDependency := j.getDummyJob("job_with_no_dependency", "destination2", namespace)

	insertRecords(j.db, []*postgres.Project{project})
	insertRecords(j.db, []*postgres.Namespace{namespace})
	insertRecords(j.db, []*postgres.Job{jobWithDependency, jobWithNoDependency})

	jobSource := &postgres.JobSource{JobID: jobWithDependency.ID, ResourceURN: jobWithNoDependency.Destination, ProjectID: project.ID}
	insertRecords(j.db, []*postgres.JobSource{jobSource})

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
		j.Len(actualInferredDependenciesPerJobID[jobWithDependency.ID], 1)
		j.Equal(jobWithNoDependency.Name, actualInferredDependenciesPerJobID[jobWithDependency.ID][0].Name)
	})
}

func (j *JobSpecRepositoryTestSuite) TestGetStaticDependenciesPerJobID() {
	project := j.getDummyProject("project_test")
	namespace := j.getDummyNamespace("namespace_test", project)
	jobWithDependency := j.getDummyJob("job_with_dependency", "destination1", namespace)
	jobWithNoDependency := j.getDummyJob("job_with_no_dependency", "destination2", namespace)

	jobDependencies, _ := json.Marshal(map[string]models.JobSpecDependency{
		jobWithNoDependency.Name: {Type: models.JobSpecDependencyTypeIntra},
	})
	jobWithDependency.Dependencies = jobDependencies

	insertRecords(j.db, []*postgres.Project{project})
	insertRecords(j.db, []*postgres.Namespace{namespace})
	insertRecords(j.db, []*postgres.Job{jobWithDependency, jobWithNoDependency})

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
		j.Len(actualInferredDependenciesPerJobID[jobWithDependency.ID], 1)
		j.Equal(jobWithNoDependency.Name, actualInferredDependenciesPerJobID[jobWithDependency.ID][0].Name)
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
		execUnit := &mock.YamlMod{}
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
				Unit: &models.Plugin{YamlMod: execUnit},
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
		execUnit := &mock.YamlMod{}
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
				Unit: &models.Plugin{YamlMod: execUnit},
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
