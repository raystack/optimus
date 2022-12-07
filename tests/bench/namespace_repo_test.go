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

	"github.com/odpf/optimus/internal/store"
	"github.com/odpf/optimus/internal/store/postgres"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkNamespaceRepository(b *testing.B) {
	ctx := context.Background()
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	proj := setup.Project(1)
	proj.ID = models.ProjectID(uuid.New())

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		err := projRepo.Save(ctx, proj)
		assert.Nil(b, err)

		secretRepo := postgres.NewSecretRepository(dbConn, hash)
		for s := 0; s < 5; s++ {
			secret := setup.Secret(s)
			err = secretRepo.Save(ctx, proj, models.NamespaceSpec{}, secret)
			assert.Nil(b, err)
		}

		return dbConn
	}

	b.Run("Save", func(b *testing.B) {
		db := dbSetup()

		var nsRepo store.NamespaceRepository = postgres.NewNamespaceRepository(db, hash)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			namespace := setup.Namespace(i, proj)
			err := nsRepo.Save(ctx, proj, namespace)
			if err != nil {
				panic(err)
			}
		}
	})
	b.Run("GetByName", func(b *testing.B) {
		db := dbSetup()

		var nsRepo store.NamespaceRepository = postgres.NewNamespaceRepository(db, hash)
		for i := 0; i < 20; i++ {
			namespace := setup.Namespace(i, proj)
			err := nsRepo.Save(ctx, proj, namespace)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 20
			nsName := fmt.Sprintf("ns-optimus-%d", num)
			namespace, err := nsRepo.GetByName(ctx, proj, nsName)
			if err != nil {
				panic(err)
			}
			if namespace.Name != nsName {
				panic("Namespace name is not same")
			}
		}
	})
	b.Run("GetAllWithUpstreams", func(b *testing.B) {
		db := dbSetup()
		var repo store.NamespaceRepository = postgres.NewNamespaceRepository(db, hash)
		for i := 0; i < 10; i++ {
			namespace := setup.Namespace(i, proj)
			err := repo.Save(ctx, proj, namespace)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			namespaces, err := repo.GetAll(ctx, proj)
			if err != nil {
				panic(err)
			}
			if len(namespaces) != 10 {
				panic("Namespaces list is not same")
			}
		}
	})
	b.Run("Get", func(b *testing.B) {
		db := dbSetup()
		var repo store.NamespaceRepository = postgres.NewNamespaceRepository(db, hash)
		for i := 0; i < 20; i++ {
			namespace := setup.Namespace(i, proj)
			err := repo.Save(ctx, proj, namespace)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 20
			nsName := fmt.Sprintf("ns-optimus-%d", num)
			namespace, err := repo.Get(ctx, proj.Name, nsName)
			if err != nil {
				panic(err)
			}
			if namespace.Name != nsName {
				panic("Namespaces is not same")
			}
		}
	})
}
