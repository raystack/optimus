// +build !unit_test

package postgres

import (
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/mock"
	"github.com/odpf/optimus/models"
)

func TestInstanceRepository(t *testing.T) {
	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
		Name: "t-optimus-id",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}

	gTask := "g-task"
	tTask := "t-task"
	execUnit1 := new(mock.Transformer)
	execUnit1.On("Name").Return(gTask)
	execUnit2 := new(mock.Transformer)
	execUnit2.On("Name").Return(tTask)

	allTasksRepo := new(mock.SupportedTransformationRepo)
	allTasksRepo.On("GetByName", gTask).Return(execUnit1, nil)
	allTasksRepo.On("GetByName", tTask).Return(execUnit2, nil)
	adapter := NewAdapter(allTasksRepo, nil)

	jobConfigs := []models.JobSpec{
		{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "g-optimus-id",
			Task: models.JobSpecTask{
				Unit: execUnit1,
				Config: []models.JobSpecConfigItem{
					{
						"do", "this",
					},
				},
			},
			Assets: *models.JobAssets{}.New(
				[]models.JobSpecAsset{
					{
						Name:  "query.sql",
						Value: "select * from 1",
					},
				}),
		},
		{
			ID:   uuid.Must(uuid.NewRandom()),
			Name: "t-optimus-id",
			Task: models.JobSpecTask{
				Unit: execUnit2,
				Config: []models.JobSpecConfigItem{
					{
						"do", "this",
					},
				},
			},
		},
	}

	unitData := models.GenerateDestinationRequest{Config: jobConfigs[0].Task.Config, Assets: jobConfigs[0].Assets.ToMap()}
	execUnit1.On("GenerateDestination", unitData).Return(models.GenerateDestinationResponse{Destination: "p.d.t"}, nil)
	unitData2 := models.GenerateDestinationRequest{Config: jobConfigs[1].Task.Config, Assets: jobConfigs[1].Assets.ToMap()}
	execUnit2.On("GenerateDestination", unitData2).Return(models.GenerateDestinationResponse{Destination: "p.d.t"}, nil)

	DBSetup := func() *gorm.DB {
		dbURL, ok := os.LookupEnv("TEST_OPTIMUS_DB_URL")
		if !ok {
			panic("unable to find TEST_OPTIMUS_DB_URL env var")
		}
		dbConn, err := Connect(dbURL, 1, 1)
		if err != nil {
			panic(err)
		}
		m, err := NewHTTPFSMigrator(dbURL)
		if err != nil {
			panic(err)
		}
		if err := m.Drop(); err != nil {
			panic(err)
		}
		if err := Migrate(dbURL); err != nil {
			panic(err)
		}

		hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")
		prepo := NewProjectRepository(dbConn, hash)
		assert.Nil(t, prepo.Save(projectSpec))

		jrepo := NewJobRepository(dbConn, projectSpec, adapter)
		assert.Nil(t, jrepo.Save(jobConfigs[0]))
		assert.Nil(t, jrepo.Save(jobConfigs[1]))
		return dbConn
	}

	testSpecs := []models.InstanceSpec{
		{
			ID:          uuid.Must(uuid.NewRandom()),
			Job:         jobConfigs[0],
			State:       models.InstanceStateSuccess,
			ScheduledAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
			Data: []models.InstanceSpecData{
				{Name: "dstart", Value: "2020-01-02", Type: models.InstanceDataTypeEnv},
			},
		},
		{
			ID:  uuid.Must(uuid.NewRandom()),
			Job: jobConfigs[1],
		},
	}

	t.Run("Insert", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()

		testModels := []models.InstanceSpec{}
		testModels = append(testModels, testSpecs...)

		iRepo1 := NewInstanceRepository(db, testModels[0].Job, adapter)
		err := iRepo1.Insert(testModels[0])
		assert.Nil(t, err)

		checkModel, err := iRepo1.GetByScheduledAt(testModels[0].ScheduledAt)
		assert.Nil(t, err)
		assert.Equal(t, testModels[0].Job.Name, checkModel.Job.Name)
		assert.Equal(t, testModels[0].Data, checkModel.Data)

		iRepo2 := NewInstanceRepository(db, testModels[1].Job, adapter)
		err = iRepo2.Insert(testModels[1])
		assert.NotNil(t, err)
	})
	t.Run("Save", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()

		testModels := []models.InstanceSpec{}
		testModels = append(testModels, testSpecs...)

		iRepo1 := NewInstanceRepository(db, testModels[0].Job, adapter)
		err := iRepo1.Save(testModels[0])
		assert.Nil(t, err)

		checkModel, err := iRepo1.GetByScheduledAt(testModels[0].ScheduledAt)
		assert.Nil(t, err)
		assert.Equal(t, testModels[0].Job.Name, checkModel.Job.Name)
		assert.Equal(t, testModels[0].Data, checkModel.Data)

		err = iRepo1.Clear(testModels[0].ScheduledAt)
		assert.Nil(t, err)

		err = iRepo1.Save(testModels[0])
		assert.Nil(t, err)

		checkModel, err = iRepo1.GetByScheduledAt(testModels[0].ScheduledAt)
		assert.Nil(t, err)
		assert.Equal(t, testModels[0].Job.Name, checkModel.Job.Name)
		assert.Equal(t, testModels[0].Data, checkModel.Data)
	})
	t.Run("Clear", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()

		testModels := []models.InstanceSpec{}
		testModels = append(testModels, testSpecs...)

		iRepo1 := NewInstanceRepository(db, testModels[0].Job, adapter)
		err := iRepo1.Save(testModels[0])
		assert.Nil(t, err)

		checkModel, err := iRepo1.GetByScheduledAt(testModels[0].ScheduledAt)
		assert.Nil(t, err)
		assert.Equal(t, testModels[0].Data, checkModel.Data)

		err = iRepo1.Clear(testModels[0].ScheduledAt)
		assert.Nil(t, err)

		checkModel, err = iRepo1.GetByScheduledAt(testModels[0].ScheduledAt)
		assert.Nil(t, err)
		assert.Equal(t, []models.InstanceSpecData{}, checkModel.Data)
	})
}
