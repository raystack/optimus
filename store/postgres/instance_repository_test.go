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
		return dbConn
	}

	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
		Name: "t-optimus-id",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}

	gTask := "g-task"
	tTask := "t-task"
	execUnit1 := new(mock.ExecutionUnit)
	execUnit1.On("GetName").Return(gTask)
	execUnit2 := new(mock.ExecutionUnit)

	allTasksRepo := new(mock.SupportedTaskRepo)
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
			Name: "",
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
	testSpecs := []models.InstanceSpec{
		{
			ID:          uuid.Must(uuid.NewRandom()),
			Job:         jobConfigs[0],
			State:       models.InstanceStateSuccess,
			ScheduledAt: time.Date(2020, 11, 11, 0, 0, 0, 0, time.UTC),
			Data:        []models.InstanceSpecData{},
		},
		{
			ID:  uuid.Must(uuid.NewRandom()),
			Job: jobConfigs[1],
		},
	}

	unitData := models.UnitData{Config: jobConfigs[0].Task.Config, Assets: jobConfigs[0].Assets.ToMap()}
	execUnit1.On("GenerateDestination", unitData).Return("p.d.t", nil)
	defer execUnit1.AssertExpectations(t)
	defer execUnit2.AssertExpectations(t)

	t.Run("Insert", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()

		testModels := []models.InstanceSpec{}
		testModels = append(testModels, testSpecs...)

		jobRepo := NewJobRepository(db, projectSpec, adapter)
		err := jobRepo.Insert(testModels[0].Job)
		assert.Nil(t, err)

		err = NewInstanceRepository(db, testModels[0].Job, adapter).Insert(testModels[0])
		assert.Nil(t, err)

		checkModel, err := NewInstanceRepository(db, testModels[0].Job, adapter).GetByScheduledAt(testModels[0].ScheduledAt)
		assert.Nil(t, err)
		assert.Equal(t, testModels[0].Job.Name, checkModel.Job.Name)
	})

}
