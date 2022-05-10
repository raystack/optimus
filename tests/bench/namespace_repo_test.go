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

	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
	"github.com/odpf/optimus/store/postgres"
)

func BenchmarkNamespaceRepository(b *testing.B) {
	ctx := context.Background()
	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	proj := getProject(1)
	proj.ID = models.ProjectID(uuid.New())

	dbSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, hash)
		err := projRepo.Save(ctx, proj)
		assert.Nil(b, err)

		secretRepo := postgres.NewSecretRepository(dbConn, hash)
		for s := 0; s < 5; s++ {
			secret := getSecret(s)
			err = secretRepo.Save(ctx, proj, models.NamespaceSpec{}, secret)
			assert.Nil(b, err)
		}

		return dbConn
	}

	b.Run("Save", func(b *testing.B) {
		db := dbSetup()

		var nsRepo store.NamespaceRepository = postgres.NewNamespaceRepository(db, proj, hash)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			namespace := getNamespace(i, proj)
			err := nsRepo.Save(ctx, namespace)
			if err != nil {
				panic(err)
			}
		}
	})
	b.Run("GetByName", func(b *testing.B) {
		db := dbSetup()

		var nsRepo store.NamespaceRepository = postgres.NewNamespaceRepository(db, proj, hash)
		for i := 0; i < 20; i++ {
			namespace := getNamespace(i, proj)
			err := nsRepo.Save(ctx, namespace)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 20
			nsName := fmt.Sprintf("ns-optimus-%d", num)
			namespace, err := nsRepo.GetByName(ctx, nsName)
			if err != nil {
				panic(err)
			}
			if namespace.Name != nsName {
				panic("Namespace name is not same")
			}
		}
	})
	b.Run("GetAll", func(b *testing.B) {
		db := dbSetup()
		var repo store.NamespaceRepository = postgres.NewNamespaceRepository(db, proj, hash)
		for i := 0; i < 10; i++ {
			namespace := getNamespace(i, proj)
			err := repo.Save(ctx, namespace)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			namespaces, err := repo.GetAll(ctx)
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
		var repo store.NamespaceRepository = postgres.NewNamespaceRepository(db, proj, hash)
		for i := 0; i < 20; i++ {
			namespace := getNamespace(i, proj)
			err := repo.Save(ctx, namespace)
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

func getNamespace(i int, project models.ProjectSpec) models.NamespaceSpec {
	return models.NamespaceSpec{
		Name: fmt.Sprintf("ns-optimus-%d", i),
		Config: map[string]string{
			"environment":                   "production",
			"bucket":                        "gs://some_folder-2",
			"storage_path":                  "gs://storage_bucket",
			"predator_host":                 "https://predator.example.com",
			"scheduler_host":                "https://optimus.example.com/",
			"transporter_kafka_brokers":     "10.5.5.5:6666",
			"transporter_stencil_namespace": "optimus",
			"bq2email_smtp_address":         "smtp.example.com",
			"bridge_host":                   "1.1.1.1",
			"bridge_port":                   "80",
			"ocean_gcs_tmp_bucket":          "bq2-plugins",
		},
		ProjectSpec: project,
	}
}
