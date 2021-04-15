// +build !unit_test

package postgres

import (
	"os"
	"testing"

	"github.com/odpf/optimus/mock"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/stretchr/testify/assert"
	"github.com/odpf/optimus/models"
)

func TestResourceSpecRepository(t *testing.T) {
	projectSpec := models.ProjectSpec{
		ID:   uuid.Must(uuid.NewRandom()),
		Name: "t-optimus-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	// prepare mocked datastore
	dsTypeTableAdapter := new(mock.DatastoreTypeAdapter)

	dsTypeTableController := new(mock.DatastoreTypeController)
	dsTypeTableController.On("Adapter").Return(dsTypeTableAdapter)

	dsController := map[models.ResourceType]models.DatastoreTypeController{
		models.ResourceTypeTable: dsTypeTableController,
	}
	datastorer := new(mock.Datastorer)
	datastorer.On("Types").Return(dsController)
	datastorer.On("Name").Return("DS")

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

		projRepo := NewProjectRepository(dbConn, hash)
		assert.Nil(t, projRepo.Save(projectSpec))
		return dbConn
	}
	testConfigs := []models.ResourceSpec{
		{
			ID:        uuid.Must(uuid.NewRandom()),
			Version:   1,
			Name:      "proj.datas.test",
			Type:      models.ResourceTypeTable,
			Datastore: datastorer,
			Spec:      nil,
			Assets: map[string]string{
				"query.sql": "select * from 1",
			},
		},
		{
			Name: "",
		},
		{
			ID:        uuid.Must(uuid.NewRandom()),
			Version:   1,
			Name:      "proj.ttt.test2",
			Type:      models.ResourceTypeTable,
			Datastore: datastorer,
			Spec:      nil,
		},
	}
	testConfigWithoutAssets := []models.ResourceSpec{
		{
			ID:        testConfigs[0].ID,
			Version:   1,
			Name:      "proj.datas.test",
			Type:      models.ResourceTypeTable,
			Datastore: datastorer,
			Spec:      nil,
		},
	}

	dsTypeTableAdapter.On("ToYaml", testConfigWithoutAssets[0]).Return([]byte("some binary data"), nil)
	dsTypeTableAdapter.On("FromYaml", []byte("some binary data")).Return(testConfigWithoutAssets[0], nil)
	dsTypeTableAdapter.On("ToYaml", testConfigs[2]).Return([]byte("some binary data X"), nil)
	dsTypeTableAdapter.On("FromYaml", []byte("some binary data X")).Return(testConfigs[2], nil)

	t.Run("Insert", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []models.ResourceSpec{}
		testModels = append(testModels, testConfigs...)

		repo := NewResourceSpecRepository(db, projectSpec, datastorer)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)

		err = repo.Insert(testModels[1])
		assert.NotNil(t, err)

		checkModel, err := repo.GetByID(testModels[0].ID)
		assert.Nil(t, err)
		assert.Equal(t, "proj.datas.test", checkModel.Name)
	})
	t.Run("Upsert", func(t *testing.T) {
		t.Run("insert different resource should insert two", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[0]
			testModelB := testConfigs[2]

			repo := NewResourceSpecRepository(db, projectSpec, datastorer)

			//try for create
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "proj.datas.test", checkModel.Name)

			//try for create
			err = repo.Save(testModelB)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModelB.ID)
			assert.Nil(t, err)
			assert.Equal(t, "proj.ttt.test2", checkModel.Name)
			assert.Equal(t, "table", checkModel.Type.String())
		})
		t.Run("insert same resource twice should overwrite existing", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			testModelA := testConfigs[2]

			repo := NewResourceSpecRepository(db, projectSpec, datastorer)

			//try for create
			err := repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err := repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, "proj.ttt.test2", checkModel.Name)

			//try for update
			testModelA.Version = 6
			dsTypeTableAdapter.On("ToYaml", testModelA).Return([]byte("some binary data testModelA"), nil)
			dsTypeTableAdapter.On("FromYaml", []byte("some binary data testModelA")).Return(testModelA, nil)

			err = repo.Save(testModelA)
			assert.Nil(t, err)

			checkModel, err = repo.GetByID(testModelA.ID)
			assert.Nil(t, err)
			assert.Equal(t, 6, checkModel.Version)
		})
		t.Run("upsert without ID should auto generate it", func(t *testing.T) {
			db := DBSetup()
			defer db.Close()
			emptyUUID := testConfigWithoutAssets[0]
			emptyUUID.ID = uuid.Nil

			dsTypeTableAdapterLocal := new(mock.DatastoreTypeAdapter)

			dsTypeTableControllerLocal := new(mock.DatastoreTypeController)
			dsTypeTableControllerLocal.On("Adapter").Return(dsTypeTableAdapterLocal)

			dsControllerLocal := map[models.ResourceType]models.DatastoreTypeController{
				models.ResourceTypeTable: dsTypeTableControllerLocal,
			}
			datastorerLocal := new(mock.Datastorer)
			datastorerLocal.On("Types").Return(dsControllerLocal)
			datastorerLocal.On("Name").Return("DS")
			emptyUUID.Datastore = datastorerLocal

			dsTypeTableAdapterLocal.On("ToYaml", emptyUUID).Return([]byte("some binary data emptyUUID nil"), nil)
			dsTypeTableAdapterLocal.On("FromYaml", []byte("some binary data emptyUUID nil")).Return(emptyUUID, nil)

			repo := NewResourceSpecRepository(db, projectSpec, datastorerLocal)

			//try for create
			err := repo.Save(emptyUUID)
			assert.Nil(t, err)

			checkModel, err := repo.GetByName(emptyUUID.Name)
			assert.Nil(t, err)
			assert.Equal(t, "proj.datas.test", checkModel.Name)
		})
	})
	t.Run("GetByName", func(t *testing.T) {
		db := DBSetup()
		defer db.Close()
		testModels := []models.ResourceSpec{}
		testModels = append(testModels, testConfigs...)

		repo := NewResourceSpecRepository(db, projectSpec, datastorer)

		err := repo.Insert(testModels[0])
		assert.Nil(t, err)

		checkModel, err := repo.GetByName(testModels[0].Name)
		assert.Nil(t, err)
		assert.Equal(t, "proj.datas.test", checkModel.Name)
	})
}
