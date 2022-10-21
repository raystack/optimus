//go:build !unit_test

package tenant_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"

	"github.com/odpf/optimus/core/tenant"
	postgres "github.com/odpf/optimus/internal/store/postgres/tenant"
	"github.com/odpf/optimus/tests/setup"
)

func TestPostgresSecretRepository(t *testing.T) {
	ctx := context.Background()

	proj, _ := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})
	namespace, _ := tenant.NewNamespace("test-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	otherNamespace, _ := tenant.NewNamespace("other-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})

	dbSetup := func() *gorm.DB {
		dbConn := setup.TestDB()
		setup.TruncateTables(dbConn)

		projRepo := postgres.NewProjectRepository(dbConn)
		assert.Nil(t, projRepo.Save(ctx, proj))

		namespaceRepo := postgres.NewNamespaceRepository(dbConn)
		assert.Nil(t, namespaceRepo.Save(ctx, namespace))
		assert.Nil(t, namespaceRepo.Save(ctx, otherNamespace))

		return dbConn
	}

	t.Run("Save", func(t *testing.T) {
		t.Run("inserts the secret without namespace set", func(t *testing.T) {
			db := dbSetup()
			projectOnlyTenant, _ := tenant.NewTenant(proj.Name().String(), "")

			validSecret, err := tenant.NewSecret("secret_name", tenant.UserDefinedSecret, "abcd", projectOnlyTenant)
			assert.Nil(t, err)

			repo := postgres.NewSecretRepository(db)

			err = repo.Save(ctx, projectOnlyTenant, validSecret)
			assert.Nil(t, err)

			secret, err := repo.Get(ctx, projectOnlyTenant, validSecret.Name())
			assert.Nil(t, err)
			assert.Equal(t, validSecret.Name(), secret.Name())
			assert.Equal(t, tenant.UserDefinedSecret, secret.Type())
			assert.Equal(t, validSecret.EncodedValue(), secret.EncodedValue())

			_, err = secret.Tenant().NamespaceName()
			assert.NotNil(t, err)
		})
		t.Run("inserts the secret with namespace set", func(t *testing.T) {
			db := dbSetup()
			tnnt, _ := tenant.NewTenant(proj.Name().String(), namespace.Name().String())

			validSecret, err := tenant.NewSecret("secret_name", tenant.UserDefinedSecret, "abcd", tnnt)
			assert.Nil(t, err)

			repo := postgres.NewSecretRepository(db)

			err = repo.Save(ctx, tnnt, validSecret)
			assert.Nil(t, err)

			secret, err := repo.Get(ctx, tnnt, validSecret.Name())
			assert.Nil(t, err)
			assert.Equal(t, validSecret.Name(), secret.Name())
			assert.Equal(t, tenant.UserDefinedSecret, secret.Type())
			assert.Equal(t, validSecret.EncodedValue(), secret.EncodedValue())

			ns, err := secret.Tenant().NamespaceName()
			assert.Nil(t, err)
			assert.Equal(t, namespace.Name().String(), ns.String())
		})
		t.Run("returns error when same secret is inserted twice", func(t *testing.T) {
			db := dbSetup()
			tnnt, _ := tenant.NewTenant(proj.Name().String(), namespace.Name().String())

			validSecret, err := tenant.NewSecret("secret_name", tenant.UserDefinedSecret, "abcd", tnnt)
			assert.Nil(t, err)

			repo := postgres.NewSecretRepository(db)

			err = repo.Save(ctx, tnnt, validSecret)
			assert.Nil(t, err)

			secret, err := repo.Get(ctx, tnnt, validSecret.Name())
			assert.Nil(t, err)
			assert.Equal(t, validSecret.Name(), secret.Name())

			err = repo.Save(ctx, tnnt, validSecret)
			assert.NotNil(t, err)
		})
	})
	t.Run("Update", func(t *testing.T) {
		t.Run("updates an already existing resource", func(t *testing.T) {
			db := dbSetup()
			tnnt, _ := tenant.NewTenant(proj.Name().String(), namespace.Name().String())

			validSecret, err := tenant.NewSecret("secret_name", tenant.UserDefinedSecret, "abcd", tnnt)
			assert.Nil(t, err)

			repo := postgres.NewSecretRepository(db)

			err = repo.Save(ctx, tnnt, validSecret)
			assert.Nil(t, err)

			secret, err := repo.Get(ctx, tnnt, validSecret.Name())
			assert.Nil(t, err)
			assert.Equal(t, validSecret.Name(), secret.Name())
			assert.Equal(t, validSecret.EncodedValue(), secret.EncodedValue())

			updatedSecret, err := tenant.NewSecret("secret_name", tenant.UserDefinedSecret, "efgh", tnnt)
			assert.Nil(t, err)

			err = repo.Update(ctx, tnnt, updatedSecret)
			assert.Nil(t, err)

			updated, err := repo.Get(ctx, tnnt, validSecret.Name())
			assert.Nil(t, err)
			assert.Equal(t, updatedSecret.Name(), updated.Name())
			assert.Equal(t, updatedSecret.EncodedValue(), updated.EncodedValue())
		})
		t.Run("returns error when secret does not exist", func(t *testing.T) {
			db := dbSetup()
			tnnt, _ := tenant.NewTenant(proj.Name().String(), namespace.Name().String())

			repo := postgres.NewSecretRepository(db)

			updatedSecret, err := tenant.NewSecret("secret_name", tenant.UserDefinedSecret, "efgh", tnnt)
			assert.Nil(t, err)

			err = repo.Update(ctx, tnnt, updatedSecret)
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found for entity secret: unable to update, secret not found for secret_name")
		})
	})
	t.Run("Get", func(t *testing.T) {
		t.Run("returns error when record is not present", func(t *testing.T) {
			db := dbSetup()
			tnnt, _ := tenant.NewTenant(proj.Name().String(), namespace.Name().String())

			validSecret, err := tenant.NewSecret("secret_name", tenant.UserDefinedSecret, "abcd", tnnt)
			assert.Nil(t, err)

			repo := postgres.NewSecretRepository(db)

			_, err = repo.Get(ctx, tnnt, validSecret.Name())
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found for entity secret: no record for secret_name")
		})
		t.Run("returns the secret when present", func(t *testing.T) {
			db := dbSetup()
			tnnt, _ := tenant.NewTenant(proj.Name().String(), namespace.Name().String())

			validSecret, err := tenant.NewSecret("secret_name", tenant.UserDefinedSecret, "abcd", tnnt)
			assert.Nil(t, err)

			repo := postgres.NewSecretRepository(db)

			err = repo.Save(ctx, tnnt, validSecret)
			assert.Nil(t, err)

			secret, err := repo.Get(ctx, tnnt, validSecret.Name())
			assert.Nil(t, err)
			assert.Equal(t, validSecret.Name(), secret.Name())
			assert.Equal(t, tenant.UserDefinedSecret, secret.Type())
			assert.Equal(t, validSecret.EncodedValue(), secret.EncodedValue())

			assert.Equal(t, proj.Name().String(), secret.Tenant().ProjectName().String())

			ns, err := secret.Tenant().NamespaceName()
			assert.Nil(t, err)
			assert.Equal(t, namespace.Name().String(), ns.String())
		})
		t.Run("should get all the secrets info for a project", func(t *testing.T) {
			db := dbSetup()

			repo := postgres.NewSecretRepository(db)

			tnnt1, _ := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
			secret1, err := tenant.NewSecret("secret_name1", tenant.UserDefinedSecret, "abcd", tnnt1)
			assert.Nil(t, err)
			err = repo.Save(ctx, tnnt1, secret1)
			assert.Nil(t, err)

			projectScope, _ := tenant.NewTenant(proj.Name().String(), "")
			secret2, err := tenant.NewSecret("secret_name3", tenant.UserDefinedSecret, "abcd", projectScope)
			assert.Nil(t, err)
			err = repo.Save(ctx, projectScope, secret2)
			assert.Nil(t, err)

			secret, err := repo.Get(ctx, tnnt1, "secret_name3")
			assert.NoError(t, err)
			assert.NotNil(t, secret)
			assert.Equal(t, "secret_name3", secret.Name().String())
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		t.Run("returns all the secrets for a project", func(t *testing.T) {
			db := dbSetup()

			repo := postgres.NewSecretRepository(db)

			tnnt1, _ := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
			secret1, err := tenant.NewSecret("secret_name1", tenant.UserDefinedSecret, "abcd", tnnt1)
			assert.Nil(t, err)
			err = repo.Save(ctx, tnnt1, secret1)
			assert.Nil(t, err)

			tnnt2, _ := tenant.NewTenant(proj.Name().String(), otherNamespace.Name().String())
			secret2, err := tenant.NewSecret("secret_name2", tenant.UserDefinedSecret, "abcd", tnnt2)
			assert.Nil(t, err)
			err = repo.Save(ctx, tnnt2, secret2)
			assert.Nil(t, err)

			projectScope, _ := tenant.NewTenant(proj.Name().String(), "")
			secret3, err := tenant.NewSecret("secret_name3", tenant.UserDefinedSecret, "abcd", projectScope)
			assert.Nil(t, err)
			err = repo.Save(ctx, projectScope, secret3)
			assert.Nil(t, err)

			secrets, err := repo.GetAll(ctx, projectScope)
			assert.Nil(t, err)

			assert.Equal(t, 3, len(secrets))

			assert.Equal(t, secret1.Name(), secrets[0].Name())
			assert.Equal(t, secret2.Name(), secrets[1].Name())
			assert.Equal(t, secret3.Name(), secrets[2].Name())
		})
		t.Run("returns secrets for current namespace and ones without namespace in project", func(t *testing.T) {
			db := dbSetup()

			repo := postgres.NewSecretRepository(db)

			tnnt1, _ := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
			secret1, err := tenant.NewSecret("secret_name1", tenant.UserDefinedSecret, "abcd", tnnt1)
			assert.Nil(t, err)
			err = repo.Save(ctx, tnnt1, secret1)
			assert.Nil(t, err)

			tnnt2, _ := tenant.NewTenant(proj.Name().String(), otherNamespace.Name().String())
			secret2, err := tenant.NewSecret("secret_name2", tenant.UserDefinedSecret, "abcd", tnnt2)
			assert.Nil(t, err)
			err = repo.Save(ctx, tnnt2, secret2)
			assert.Nil(t, err)

			projectScope, _ := tenant.NewTenant(proj.Name().String(), "")
			secret3, err := tenant.NewSecret("secret_name3", tenant.UserDefinedSecret, "abcd", projectScope)
			assert.Nil(t, err)
			err = repo.Save(ctx, projectScope, secret3)
			assert.Nil(t, err)

			secrets, err := repo.GetAll(ctx, tnnt1)
			assert.Nil(t, err)

			assert.Equal(t, 2, len(secrets))

			assert.Equal(t, secret1.Name(), secrets[0].Name())
			assert.Equal(t, secret3.Name(), secrets[1].Name())
		})
	})
	t.Run("Delete", func(t *testing.T) {
		t.Run("deletes the secret for namespace", func(t *testing.T) {
			db := dbSetup()
			tnnt, _ := tenant.NewTenant(proj.Name().String(), namespace.Name().String())

			validSecret, err := tenant.NewSecret("secret_name", tenant.UserDefinedSecret, "abcd", tnnt)
			assert.Nil(t, err)

			repo := postgres.NewSecretRepository(db)

			err = repo.Save(ctx, tnnt, validSecret)
			assert.Nil(t, err)

			err = repo.Delete(ctx, tnnt, validSecret.Name())
			assert.Nil(t, err)

			_, err = repo.Get(ctx, tnnt, validSecret.Name())
			assert.NotNil(t, err)
		})
		t.Run("deletes the secret for project", func(t *testing.T) {
			db := dbSetup()
			tnnt, _ := tenant.NewTenant(proj.Name().String(), "")

			validSecret, err := tenant.NewSecret("secret_name", tenant.UserDefinedSecret, "abcd", tnnt)
			assert.Nil(t, err)

			repo := postgres.NewSecretRepository(db)

			err = repo.Save(ctx, tnnt, validSecret)
			assert.Nil(t, err)

			err = repo.Delete(ctx, tnnt, validSecret.Name())
			assert.Nil(t, err)

			_, err = repo.Get(ctx, tnnt, validSecret.Name())
			assert.NotNil(t, err)
		})
		t.Run("returns error when non existing is deleted", func(t *testing.T) {
			db := dbSetup()
			tnnt, _ := tenant.NewTenant(proj.Name().String(), namespace.Name().String())

			validSecret, err := tenant.NewSecret("secret_name", tenant.UserDefinedSecret, "abcd", tnnt)
			assert.Nil(t, err)

			repo := postgres.NewSecretRepository(db)

			err = repo.Delete(ctx, tnnt, validSecret.Name())
			assert.NotNil(t, err)
			assert.EqualError(t, err, "not found for entity secret: secret to delete not found secret_name")
		})
	})
	t.Run("GetSecretsInfo", func(t *testing.T) {
		t.Run("should get all the secrets info for a project", func(t *testing.T) {
			db := dbSetup()

			repo := postgres.NewSecretRepository(db)

			tnnt1, _ := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
			secret1, err := tenant.NewSecret("secret_name1", tenant.UserDefinedSecret, "abcd", tnnt1)
			assert.Nil(t, err)
			err = repo.Save(ctx, tnnt1, secret1)
			assert.Nil(t, err)

			tnnt2, _ := tenant.NewTenant(proj.Name().String(), otherNamespace.Name().String())
			secret2, err := tenant.NewSecret("secret_name2", tenant.UserDefinedSecret, "abcd", tnnt2)
			assert.Nil(t, err)
			err = repo.Save(ctx, tnnt2, secret2)
			assert.Nil(t, err)

			projectScope, _ := tenant.NewTenant(proj.Name().String(), "")
			secret3, err := tenant.NewSecret("secret_name3", tenant.UserDefinedSecret, "abcd", projectScope)
			assert.Nil(t, err)
			err = repo.Save(ctx, projectScope, secret3)
			assert.Nil(t, err)

			projScopeSecrets, err := repo.GetSecretsInfo(ctx, projectScope)
			assert.Nil(t, err)
			assert.Equal(t, 3, len(projScopeSecrets))

			// returns only at project scope, ignores the namespace
			secretsInfo, err := repo.GetSecretsInfo(ctx, tnnt1)
			assert.Nil(t, err)
			assert.Equal(t, 3, len(secretsInfo))

			info1 := secretsInfo[0]
			assert.Equal(t, secret1.Name().String(), info1.Name)
			assert.Equal(t, tenant.UserDefinedSecret, info1.Type)
			assert.Equal(t, namespace.Name().String(), info1.Namespace)
			assert.NotNil(t, info1.UpdatedAt)
			assert.NotEmpty(t, info1.Digest)

			info2 := secretsInfo[1]
			assert.Equal(t, secret2.Name().String(), info2.Name)
			assert.Equal(t, tenant.UserDefinedSecret, info2.Type)
			assert.Equal(t, otherNamespace.Name().String(), info2.Namespace)
			assert.NotNil(t, info2.UpdatedAt)
			assert.NotEmpty(t, info2.Digest)

			info3 := secretsInfo[2]
			assert.Equal(t, secret3.Name().String(), info3.Name)
			assert.Equal(t, tenant.UserDefinedSecret, info3.Type)
			assert.Equal(t, "", info3.Namespace)
			assert.NotNil(t, info3.UpdatedAt)
			assert.NotEmpty(t, info3.Digest)
		})
	})
}
