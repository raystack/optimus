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

	b.Run("Insert", func(b *testing.B) {
		dbConn := dbSetup()
		repo := postgres.NewSecretRepository(dbConn, key)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			secret := models.ProjectSecretItem{
				Name:  fmt.Sprintf("Secret%d", i),
				Value: "secret",
				Type:  models.SecretTypeUserDefined,
			}

			err := repo.Save(ctx, project, namespace, secret)
			assert.Nil(b, err)
		}
	})
}
