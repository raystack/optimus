//go:build !unit_test
// +build !unit_test

package tenant

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	serviceTenant "github.com/odpf/optimus/core/tenant"
	repoTenant "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func BenchmarkSecretRepository(b *testing.B) {
	ctx := context.Background()

	proj, err := serviceTenant.NewProject("test-proj",
		map[string]string{
			"bucket":                            "gs://some_folder-2",
			serviceTenant.ProjectSchedulerHost:  "host",
			serviceTenant.ProjectStoragePathKey: "gs://location",
		})
	assert.NoError(b, err)
	namespace, err := serviceTenant.NewNamespace("test-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	assert.NoError(b, err)

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		prjRepo := repoTenant.NewProjectRepository(dbConn)
		if err := prjRepo.Save(ctx, proj); err != nil {
			panic(err)
		}

		namespaceRepo := repoTenant.NewNamespaceRepository(dbConn)
		if err := namespaceRepo.Save(ctx, namespace); err != nil {
			panic(err)
		}
		return dbConn
	}

	b.Run("Save", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewSecretRepository(db)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			name := fmt.Sprintf("secret_name_%d", i)
			secret, err := serviceTenant.NewSecret(name, serviceTenant.UserDefinedSecret, "abcd", proj.Name(), namespace.Name().String())
			assert.Nil(b, err)

			actualError := repo.Save(ctx, secret)
			assert.NoError(b, actualError)
		}
	})

	b.Run("Update", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewSecretRepository(db)
		maxNumberOfSecrets := 50
		for i := 0; i < maxNumberOfSecrets; i++ {
			name := fmt.Sprintf("secret_name_%d", i)
			secret, err := serviceTenant.NewSecret(name, serviceTenant.UserDefinedSecret, "abcd", proj.Name(), namespace.Name().String())
			assert.Nil(b, err)

			actualError := repo.Save(ctx, secret)
			assert.NoError(b, actualError)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			secretIdx := i % maxNumberOfSecrets
			name := fmt.Sprintf("secret_name_%d", secretIdx)
			secret, err := serviceTenant.NewSecret(name, serviceTenant.UserDefinedSecret, "abcd", proj.Name(), namespace.Name().String())
			assert.Nil(b, err)

			actualError := repo.Update(ctx, secret)
			assert.NoError(b, actualError)
		}
	})

	b.Run("Get", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewSecretRepository(db)
		maxNumberOfSecrets := 50
		for i := 0; i < maxNumberOfSecrets; i++ {
			name := fmt.Sprintf("secret_name_%d", i)
			secret, err := serviceTenant.NewSecret(name, serviceTenant.UserDefinedSecret, "abcd", proj.Name(), namespace.Name().String())
			assert.Nil(b, err)

			actualError := repo.Save(ctx, secret)
			assert.NoError(b, actualError)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			secretIdx := i % maxNumberOfSecrets
			name := fmt.Sprintf("secret_name_%d", secretIdx)
			secretName, err := serviceTenant.SecretNameFrom(name)
			assert.NoError(b, err)

			actualSecret, actualError := repo.Get(ctx, proj.Name(), namespace.Name().String(), secretName)
			assert.NotNil(b, actualSecret)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewSecretRepository(db)
		maxNumberOfSecrets := 50
		for i := 0; i < maxNumberOfSecrets; i++ {
			name := fmt.Sprintf("secret_name_%d", i)
			secret, err := serviceTenant.NewSecret(name, serviceTenant.UserDefinedSecret, "abcd", proj.Name(), namespace.Name().String())
			assert.Nil(b, err)

			actualError := repo.Save(ctx, secret)
			assert.NoError(b, actualError)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualSecrets, actualError := repo.GetAll(ctx, proj.Name(), namespace.Name().String())
			assert.Len(b, actualSecrets, maxNumberOfSecrets)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetSecrets", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewSecretRepository(db)
		maxNumberOfSecrets := 50
		for i := 0; i < maxNumberOfSecrets; i++ {
			name := fmt.Sprintf("secret_name_%d", i)
			secret, err := serviceTenant.NewSecret(name, serviceTenant.UserDefinedSecret, "abcd", proj.Name(), namespace.Name().String())
			assert.Nil(b, err)

			actualError := repo.Save(ctx, secret)
			assert.NoError(b, actualError)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualSecretInfos, actualError := repo.GetSecretsInfo(ctx, proj.Name())
			assert.Len(b, actualSecretInfos, maxNumberOfSecrets)
			assert.NoError(b, actualError)
		}
	})

	b.Run("Delete", func(b *testing.B) {
		db := dbSetup()
		repo := repoTenant.NewSecretRepository(db)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			name := fmt.Sprintf("secret_name_%d", i)
			secret, err := serviceTenant.NewSecret(name, serviceTenant.UserDefinedSecret, "abcd", proj.Name(), namespace.Name().String())
			assert.Nil(b, err)

			err = repo.Save(ctx, secret)
			assert.NoError(b, err)

			actualError := repo.Delete(ctx, proj.Name(), namespace.Name().String(), secret.Name())
			assert.NoError(b, actualError)
		}
	})
}
