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

func BenchmarkProjectRepository(b *testing.B) {
	ctx := context.Background()
	dbSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		return dbConn
	}

	hash, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	b.Run("Save", func(b *testing.B) {
		db := dbSetup()
		var repo store.ProjectRepository = postgres.NewProjectRepository(db, hash)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			project := getProject(i)
			err := repo.Save(ctx, project)
			if err != nil {
				panic(err)
			}
		}
	})

	b.Run("GetByName", func(b *testing.B) {
		db := dbSetup()
		var repo store.ProjectRepository = postgres.NewProjectRepository(db, hash)
		secretRepo := postgres.NewSecretRepository(db, hash)
		for i := 0; i < 10; i++ {
			project := getProject(i)
			project.ID = models.ProjectID(uuid.New())
			err := repo.Save(ctx, project)
			assert.Nil(b, err)

			for s := 0; s < 5; s++ {
				secret := getSecret(s)
				err = secretRepo.Save(ctx, project, models.NamespaceSpec{}, secret)
				assert.Nil(b, err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 10
			projName := fmt.Sprintf("t-optimus-%d", num)
			prj, err := repo.GetByName(ctx, projName)
			if err != nil {
				panic(err)
			}
			if prj.Name != projName {
				panic("Project name is not same")
			}
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		db := dbSetup()
		var repo store.ProjectRepository = postgres.NewProjectRepository(db, hash)
		secretRepo := postgres.NewSecretRepository(db, hash)
		for i := 0; i < 10; i++ {
			project := getProject(i)
			project.ID = models.ProjectID(uuid.New())
			err := repo.Save(ctx, project)
			assert.Nil(b, err)

			for s := 0; s < 5; s++ {
				secret := getSecret(s)
				err = secretRepo.Save(ctx, project, models.NamespaceSpec{}, secret)
				assert.Nil(b, err)
			}
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			prj, err := repo.GetAll(ctx)
			if err != nil {
				panic(err)
			}
			if len(prj) != 10 {
				panic("Project list is not same")
			}
		}
	})
}

func getProject(i int) models.ProjectSpec {
	return models.ProjectSpec{
		Name: fmt.Sprintf("t-optimus-%d", i),
		Config: map[string]string{
			"environment":                         "production",
			"bucket":                              "gs://some_folder-2",
			"storage_path":                        "gs://storage_bucket",
			"transporterKafka":                    "10.12.12.12:6668,10.12.12.13:6668",
			"predator_host":                       "https://predator.example.com",
			"scheduler_host":                      "https://optimus.example.com/",
			"transporter_kafka_brokers":           "10.5.5.5:6666",
			"transporter_stencil_host":            "https://artifactory.example.com/artifactory/proto-descriptors/ocean-proton/latest",
			"transporter_stencil_broker_host":     "https://artifactory.example.com/artifactory/proto-descriptors/latest",
			"transporter_stencil_server_url":      "https://stencil.example.com",
			"transporter_stencil_namespace":       "optimus",
			"transporter_stencil_descriptor_name": "transporter-log-entities",
			"bq2email_smtp_address":               "smtp.example.com",
			"bridge_host":                         "1.1.1.1",
			"bridge_port":                         "80",
			"ocean_gcs_tmp_bucket":                "bq2-plugins",
		},
		Secret: models.ProjectSecrets{
			{
				Name:  "secret1",
				Value: "secret1",
				Type:  models.SecretTypeUserDefined,
			},
			{
				Name:  "secret2",
				Value: "secret2",
				Type:  models.SecretTypeUserDefined,
			},
			{
				Name:  "secret3",
				Value: "secret3",
				Type:  models.SecretTypeUserDefined,
			},
		},
	}
}
