//go:build !unit_test

package resource_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/raystack/optimus/core/resource"
	"github.com/raystack/optimus/core/tenant"
	postgres "github.com/raystack/optimus/internal/store/postgres/resource"
)

func TestPostgresBackupRepository(t *testing.T) {
	ctx := context.Background()
	tnnt, _ := tenant.NewTenant("t-optimus-1", "n-optimus-1")
	resNames := []string{"bigquery-project.playground.test-table", "bigquery-project.playground.table1"}
	created := time.Date(2022, 11, 22, 5, 0, 0, 0, time.UTC)
	conf := map[string]string{"config1": "value1", "ttl": "720h"}
	store := resource.Bigquery

	t.Run("Create", func(t *testing.T) {
		t.Run("saves the resource in database", func(t *testing.T) {
			db := dbSetup()
			backupRepo := postgres.NewBackupRepository(db)

			toBackup, err := resource.NewBackup(store, tnnt, resNames, "a backup", created, conf)
			assert.Nil(t, err)

			err = backupRepo.Create(ctx, toBackup)
			assert.Nil(t, err)

			assert.False(t, toBackup.ID().IsInvalid())
		})
	})
	t.Run("GetByID", func(t *testing.T) {
		t.Run("returns the resource by ID", func(t *testing.T) {
			db := dbSetup()
			backupRepo := postgres.NewBackupRepository(db)

			backup, err := resource.NewBackup(store, tnnt, resNames, "a backup", created, conf)
			assert.Nil(t, err)

			err = backupRepo.Create(ctx, backup)
			assert.Nil(t, err)

			fromDB, err := backupRepo.GetByID(ctx, backup.ID())
			assert.Nil(t, err)

			assert.Equal(t, backup.ID().String(), fromDB.ID().String())
			assert.Equal(t, backup.Store(), fromDB.Store())
			assert.Equal(t, backup.Tenant(), fromDB.Tenant())
			assert.Equal(t, backup.CreatedAt(), fromDB.CreatedAt())
			assert.Equal(t, backup.Description(), fromDB.Description())
			assert.Equal(t, backup.ResourceNames(), fromDB.ResourceNames())
		})
	})
	t.Run("GetAll", func(t *testing.T) {
		t.Run("returns all the backups in database", func(t *testing.T) {
			db := dbSetup()
			backupRepo := postgres.NewBackupRepository(db)

			backup1, err := resource.NewBackup(store, tnnt, resNames, "a backup", created, conf)
			assert.Nil(t, err)
			err = backupRepo.Create(ctx, backup1)
			assert.Nil(t, err)

			names := []string{"proj.dataset.table2"}
			backup2, err := resource.NewBackup(store, tnnt, names, "a backup", created, conf)
			assert.Nil(t, err)
			err = backupRepo.Create(ctx, backup2)
			assert.Nil(t, err)

			backups, err := backupRepo.GetAll(ctx, tnnt, store)
			assert.Nil(t, err)

			assert.Equal(t, 2, len(backups))
			assert.Equal(t, backup1.ID(), backups[0].ID())
			assert.Equal(t, backup2.ID(), backups[1].ID())
		})
	})
}
