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

func BenchmarkSecretRepo(b *testing.B) {
	ctx := context.Background()
	project := models.ProjectSpec{
		ID:   models.ProjectID(uuid.New()),
		Name: "t-optimus-project",
		Config: map[string]string{
			"bucket": "gs://some_folder",
		},
	}
	namespace := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "sample-namespace",
		ProjectSpec: project,
	}
	otherNamespace := models.NamespaceSpec{
		ID:          uuid.New(),
		Name:        "other-namespace",
		ProjectSpec: project,
	}

	key, _ := models.NewApplicationSecret("32charshtesthashtesthashtesthash")

	dbSetup := func() *gorm.DB {
		dbConn := setupDB()
		truncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn, key)
		assert.Nil(b, projRepo.Save(ctx, project))

		namespaceRepo := postgres.NewNamespaceRepository(dbConn, project, key)
		assert.Nil(b, namespaceRepo.Save(ctx, namespace))
		assert.Nil(b, namespaceRepo.Save(ctx, otherNamespace))
		return dbConn
	}

	b.Run("Save", func(b *testing.B) {
		dbConn := dbSetup()
		var repo store.SecretRepository = postgres.NewSecretRepository(dbConn, key)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			secret := getSecret(i)

			err := repo.Save(ctx, project, namespace, secret)
			if err != nil {
				panic(err)
			}
		}
	})

	b.Run("Update", func(b *testing.B) {
		dbConn := dbSetup()
		var repo store.SecretRepository = postgres.NewSecretRepository(dbConn, key)
		for i := 0; i < 50; i++ {
			secret := models.ProjectSecretItem{
				Name:  fmt.Sprintf("Secret%d", i),
				Value: "secret",
				Type:  models.SecretTypeUserDefined,
			}
			err := repo.Save(ctx, project, namespace, secret)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			num := i % 50
			secret := models.ProjectSecretItem{
				Name:  fmt.Sprintf("Secret%d", num),
				Value: "secret",
				Type:  models.SecretTypeUserDefined,
			}
			err := repo.Update(ctx, project, namespace, secret)
			if err != nil {
				panic(err)
			}
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		dbConn := dbSetup()
		var repo store.SecretRepository = postgres.NewSecretRepository(dbConn, key)
		for i := 0; i < 20; i++ {
			secretNS := models.ProjectSecretItem{
				Name:  fmt.Sprintf("SecretNS-%d", i),
				Value: "secret",
				Type:  models.SecretTypeUserDefined,
			}
			err := repo.Save(ctx, project, namespace, secretNS)
			assert.Nil(b, err)

			secretNONS := models.ProjectSecretItem{
				Name:  fmt.Sprintf("SecretNONS-%d", i),
				Value: "secret",
				Type:  models.SecretTypeUserDefined,
			}
			err = repo.Save(ctx, project, models.NamespaceSpec{}, secretNONS)
			assert.Nil(b, err)

			secretONS := models.ProjectSecretItem{
				Name:  fmt.Sprintf("SecretONS-%d", i),
				Value: "secret",
				Type:  models.SecretTypeUserDefined,
			}
			err = repo.Save(ctx, project, otherNamespace, secretONS)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			sec, err := repo.GetAll(ctx, project)
			if err != nil {
				panic(err)
			}
			if len(sec) != 60 {
				panic("Did not get all the secrets")
			}
		}
	})

	b.Run("GetSecrets", func(b *testing.B) {
		dbConn := dbSetup()
		var repo store.SecretRepository = postgres.NewSecretRepository(dbConn, key)
		for i := 0; i < 20; i++ {
			secretNS := models.ProjectSecretItem{
				Name:  fmt.Sprintf("SecretNS-%d", i),
				Value: "secret",
				Type:  models.SecretTypeUserDefined,
			}
			err := repo.Save(ctx, project, namespace, secretNS)
			assert.Nil(b, err)

			secretNONS := models.ProjectSecretItem{
				Name:  fmt.Sprintf("SecretNONS-%d", i),
				Value: "secret",
				Type:  models.SecretTypeUserDefined,
			}
			err = repo.Save(ctx, project, models.NamespaceSpec{}, secretNONS)
			assert.Nil(b, err)

			secretONS := models.ProjectSecretItem{
				Name:  fmt.Sprintf("SecretONS-%d", i),
				Value: "secret",
				Type:  models.SecretTypeUserDefined,
			}
			err = repo.Save(ctx, project, otherNamespace, secretONS)
			assert.Nil(b, err)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			sec, err := repo.GetSecrets(ctx, project, namespace)
			if err != nil {
				panic(err)
			}
			if len(sec) != 40 {
				panic("Did not get all the secrets")
			}
		}
	})

	b.Run("Delete", func(b *testing.B) {
		dbConn := dbSetup()
		var repo store.SecretRepository = postgres.NewSecretRepository(dbConn, key)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			secretName := fmt.Sprintf("Secret%d", i)
			secret := models.ProjectSecretItem{
				Name:  secretName,
				Value: "secret",
				Type:  models.SecretTypeUserDefined,
			}

			err := repo.Save(ctx, project, namespace, secret)
			if err != nil {
				panic(err)
			}

			err = repo.Delete(ctx, project, namespace, secretName)
			if err != nil {
				panic(err)
			}
		}
	})
}

func getSecret(i int) models.ProjectSecretItem {
	return models.ProjectSecretItem{
		Name:  fmt.Sprintf("Secret%d", i),
		Value: "secret",
		Type:  models.SecretTypeUserDefined,
	}
}
