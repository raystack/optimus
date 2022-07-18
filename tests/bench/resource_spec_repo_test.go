//go:build !unit_test
// +build !unit_test

package bench

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/ext/datastore/bigquery"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/postgres"
	"github.com/odpf/optimus/tests/setup"
)

func getResourceSpec(i int, ds models.Datastorer) models.ResourceSpec {
	return models.ResourceSpec{
		ID:        uuid.New(),
		Version:   1,
		Name:      fmt.Sprintf("proj.datas.test-%d", i),
		Type:      models.ResourceTypeTable,
		Datastore: ds,
		Spec: bigquery.BQTable{
			Project: "project",
			Dataset: "dataset",
			Table:   fmt.Sprintf("table%d", i),
		},
		Assets: map[string]string{
			"query.sql": "select * from 1",
		},
		URN: "datastore://proj:datas.test",
		Labels: map[string]string{
			"owner": "optimus",
		},
	}
}

func BenchmarkResourceSpecRepository(b *testing.B) {
	ctx := context.Background()
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	bigqueryStore := setup.MockBigQueryDataStore()

	project := setup.Project(1)
	project.ID = models.ProjectID(uuid.New())

	namespace := setup.Namespace(1, project)
	namespace.ID = uuid.New()

	DBSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		assert.Nil(b, projRepo.Save(ctx, project))

		nsRepo := postgres.NewNamespaceRepository(dbConn, hash)
		assert.Nil(b, nsRepo.Save(ctx, project, namespace))

		secretRepo := postgres.NewSecretRepository(dbConn, hash)
		for i := 0; i < 5; i++ {
			assert.Nil(b, secretRepo.Save(ctx, project, namespace, setup.Secret(i)))
		}

		return dbConn
	}

	b.Run("Save", func(b *testing.B) {
		db := DBSetup()

		projectResourceSpecRepo := postgres.NewProjectResourceSpecRepository(db, project, bigqueryStore)
		var repo store.ResourceSpecRepository = postgres.NewResourceSpecRepository(db, namespace, bigqueryStore, projectResourceSpecRepo)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			resource := getResourceSpec(i, bigqueryStore)

			err := repo.Save(ctx, resource)
			if err != nil {
				panic(err)
			}
		}
	})

	b.Run("GetByName", func(b *testing.B) {
		db := DBSetup()

		projectResourceSpecRepo := postgres.NewProjectResourceSpecRepository(db, project, bigqueryStore)
		var repo store.ResourceSpecRepository = postgres.NewResourceSpecRepository(db, namespace, bigqueryStore, projectResourceSpecRepo)

		for i := 0; i < 1000; i++ {
			resource := getResourceSpec(i, bigqueryStore)

			err := repo.Save(ctx, resource)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 1000
			name := fmt.Sprintf("proj.datas.test-%d", num)

			res, err := repo.GetByName(ctx, name)
			if err != nil {
				panic(err)
			}
			if res.Name != name {
				panic("Name is not same")
			}
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		db := DBSetup()

		projectResourceSpecRepo := postgres.NewProjectResourceSpecRepository(db, project, bigqueryStore)
		var repo store.ResourceSpecRepository = postgres.NewResourceSpecRepository(db, namespace, bigqueryStore, projectResourceSpecRepo)

		for i := 0; i < 1000; i++ {
			resource := getResourceSpec(i, bigqueryStore)

			err := repo.Save(ctx, resource)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			res, err := repo.GetAll(ctx)
			if err != nil {
				panic(err)
			}
			if len(res) != 1000 {
				panic("Resource length is not same")
			}
		}
	})
}

func BenchmarkProjectResourceSpecRepo(b *testing.B) {
	ctx := context.Background()
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	bigqueryStore := setup.MockBigQueryDataStore()

	project := setup.Project(1)
	project.ID = models.ProjectID(uuid.New())

	namespace := setup.Namespace(1, project)
	namespace.ID = uuid.New()

	DBSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		assert.Nil(b, projRepo.Save(ctx, project))

		nsRepo := postgres.NewNamespaceRepository(dbConn, hash)
		assert.Nil(b, nsRepo.Save(ctx, project, namespace))

		secretRepo := postgres.NewSecretRepository(dbConn, hash)
		for i := 0; i < 5; i++ {
			assert.Nil(b, secretRepo.Save(ctx, project, namespace, setup.Secret(i)))
		}

		return dbConn
	}

	b.Run("GetByName", func(b *testing.B) {
		db := DBSetup()

		var repo store.ProjectResourceSpecRepository = postgres.NewProjectResourceSpecRepository(db, project, bigqueryStore)
		var resourceRepo store.ResourceSpecRepository = postgres.NewResourceSpecRepository(db, namespace, bigqueryStore, repo)

		for i := 0; i < 1000; i++ {
			resource := getResourceSpec(i, bigqueryStore)

			err := resourceRepo.Save(ctx, resource)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 1000
			name := fmt.Sprintf("proj.datas.test-%d", num)

			res, _, err := repo.GetByName(ctx, name)
			if err != nil {
				panic(err)
			}
			if res.Name != name {
				panic("Name is not same")
			}
		}
	})

	b.Run("GetByURN", func(b *testing.B) {
		db := DBSetup()

		var repo store.ProjectResourceSpecRepository = postgres.NewProjectResourceSpecRepository(db, project, bigqueryStore)
		var resourceRepo store.ResourceSpecRepository = postgres.NewResourceSpecRepository(db, namespace, bigqueryStore, repo)

		for i := 0; i < 1000; i++ {
			resource := getResourceSpec(i, bigqueryStore)

			err := resourceRepo.Save(ctx, resource)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 1000
			urn := fmt.Sprintf("bigquery://project:dataset.table%d", num)

			res, _, err := repo.GetByURN(ctx, urn)
			if err != nil {
				panic(err)
			}
			if res.URN != urn {
				panic("URN is not same")
			}
		}
	})
}
