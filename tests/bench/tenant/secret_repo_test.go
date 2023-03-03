//go:build !unit_test

package tenant

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	serviceTenant "github.com/goto/optimus/core/tenant"
	repoTenant "github.com/goto/optimus/internal/store/postgres/tenant"
	"github.com/goto/optimus/tests/setup"
)

func BenchmarkSecretRepository(b *testing.B) {
	const maxNumberOfSecrets = 64

	transporterKafkaBrokerKey := "KAFKA_BROKERS"
	config := map[string]string{
		"bucket":                            "gs://folder_for_test",
		transporterKafkaBrokerKey:           "192.168.1.1:8080,192.168.1.1:8081",
		serviceTenant.ProjectSchedulerHost:  "http://localhost:8082",
		serviceTenant.ProjectStoragePathKey: "gs://location",
	}
	project, err := serviceTenant.NewProject("project_for_test", config)
	assert.NoError(b, err)
	namespace, err := serviceTenant.NewNamespace("namespace_for_test", project.Name(), config)
	assert.NoError(b, err)

	ctx := context.Background()

	dbSetup := func(b *testing.B) *pgxpool.Pool {
		b.Helper()

		pool := setup.TestPool()
		setup.TruncateTablesWith(pool)

		projectRepo := repoTenant.NewProjectRepository(pool)
		err := projectRepo.Save(ctx, project)
		assert.NoError(b, err)

		namespaceRepo := repoTenant.NewNamespaceRepository(pool)
		err = namespaceRepo.Save(ctx, namespace)
		assert.NoError(b, err)

		return pool
	}

	b.Run("Save", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoTenant.NewSecretRepository(db)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			name := fmt.Sprintf("secret_for_test_%d", i)
			encodedValue := "encoded_secret_value"
			secret, err := serviceTenant.NewSecret(name, encodedValue, project.Name(), namespace.Name().String())
			assert.NoError(b, err)

			actualError := repo.Save(ctx, secret)
			assert.NoError(b, actualError)
		}
	})

	b.Run("Update", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoTenant.NewSecretRepository(db)
		secretNames := make([]string, maxNumberOfSecrets)
		for i := 0; i < maxNumberOfSecrets; i++ {
			name := fmt.Sprintf("secret_for_test_%d", i)
			encodedValue := "encoded_secret_value"
			secret, err := serviceTenant.NewSecret(name, encodedValue, project.Name(), namespace.Name().String())
			assert.NoError(b, err)

			actualError := repo.Save(ctx, secret)
			assert.NoError(b, actualError)

			secretNames[i] = name
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			secretIdx := i % maxNumberOfSecrets
			name := secretNames[secretIdx]
			newEncodedValue := "new_encoded_secret_value"
			secret, err := serviceTenant.NewSecret(name, newEncodedValue, project.Name(), namespace.Name().String())
			assert.NoError(b, err)

			actualError := repo.Update(ctx, secret)
			assert.NoError(b, actualError)
		}
	})

	b.Run("Get", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoTenant.NewSecretRepository(db)
		secretNames := make([]string, maxNumberOfSecrets)
		for i := 0; i < maxNumberOfSecrets; i++ {
			name := fmt.Sprintf("secret_for_test_%d", i)
			encodedValue := "encoded_secret_value"
			secret, err := serviceTenant.NewSecret(name, encodedValue, project.Name(), namespace.Name().String())
			assert.NoError(b, err)

			actualError := repo.Save(ctx, secret)
			assert.NoError(b, actualError)

			secretNames[i] = name
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			secretIdx := i % maxNumberOfSecrets
			name := secretNames[secretIdx]
			secretName, err := serviceTenant.SecretNameFrom(name)
			assert.NoError(b, err)

			actualSecret, actualError := repo.Get(ctx, project.Name(), namespace.Name().String(), secretName)
			assert.NotNil(b, actualSecret)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetAll", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoTenant.NewSecretRepository(db)
		for i := 0; i < maxNumberOfSecrets; i++ {
			name := fmt.Sprintf("secret_for_test_%d", i)
			encodedValue := "encoded_secret_value"
			secret, err := serviceTenant.NewSecret(name, encodedValue, project.Name(), namespace.Name().String())
			assert.NoError(b, err)

			actualError := repo.Save(ctx, secret)
			assert.NoError(b, actualError)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualSecrets, actualError := repo.GetAll(ctx, project.Name(), namespace.Name().String())
			assert.Len(b, actualSecrets, maxNumberOfSecrets)
			assert.NoError(b, actualError)
		}
	})

	b.Run("GetSecretsInfo", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoTenant.NewSecretRepository(db)
		for i := 0; i < maxNumberOfSecrets; i++ {
			name := fmt.Sprintf("secret_for_test_%d", i)
			encodedValue := "encoded_secret_value"
			secret, err := serviceTenant.NewSecret(name, encodedValue, project.Name(), namespace.Name().String())
			assert.NoError(b, err)

			actualError := repo.Save(ctx, secret)
			assert.NoError(b, actualError)
		}

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			actualSecretInfos, actualError := repo.GetSecretsInfo(ctx, project.Name())
			assert.Len(b, actualSecretInfos, maxNumberOfSecrets)
			assert.NoError(b, actualError)
		}
	})

	b.Run("Delete", func(b *testing.B) {
		db := dbSetup(b)
		repo := repoTenant.NewSecretRepository(db)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			name := fmt.Sprintf("secret_for_test_%d", i)
			encodedValue := "encoded_secret_value"
			secret, err := serviceTenant.NewSecret(name, encodedValue, project.Name(), namespace.Name().String())
			assert.NoError(b, err)

			err = repo.Save(ctx, secret)
			assert.NoError(b, err)

			secretName, err := serviceTenant.SecretNameFrom(name)
			assert.NoError(b, err)

			actualError := repo.Delete(ctx, project.Name(), namespace.Name().String(), secretName)
			assert.NoError(b, actualError)
		}
	})
}
