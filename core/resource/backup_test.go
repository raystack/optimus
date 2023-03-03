package resource_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
)

func TestBackup(t *testing.T) {
	t.Run("BackupID", func(t *testing.T) {
		t.Run("when invalid", func(t *testing.T) {
			t.Run("returns error when string is empty", func(t *testing.T) {
				id, err := resource.BackupIDFrom("")
				assert.NotNil(t, err)
				assert.True(t, id.IsInvalid())
				assert.EqualError(t, err, "invalid argument for entity backup: invalid id for backup ")
			})
			t.Run("returns error when string is empty", func(t *testing.T) {
				id, err := resource.BackupIDFrom("abcdefghijklmnopqrstuvwxyzabcdefghij")
				assert.NotNil(t, err)
				assert.True(t, id.IsInvalid())
				assert.EqualError(t, err, "invalid argument for entity backup: invalid id for backup abcdefghijklmnopqrstuvwxyzabcdefghij")
			})
			t.Run("returns error when string is invalid", func(t *testing.T) {
				id, err := resource.BackupIDFrom("abcdefghijklmnopqrstuvwxyzabcdefghij")
				assert.NotNil(t, err)
				assert.True(t, id.IsInvalid())
				assert.EqualError(t, err, "invalid argument for entity backup: invalid id for backup abcdefghijklmnopqrstuvwxyzabcdefghij")
			})
			t.Run("returns is-invalid when backupID is nil", func(t *testing.T) {
				id, err := resource.BackupIDFrom(uuid.Nil.String())
				assert.NotNil(t, err)
				assert.True(t, id.IsInvalid())
				assert.EqualError(t, err, "invalid argument for entity backup: nil id for backup 00000000-0000-0000-0000-000000000000")
			})
		})
		t.Run("when valid", func(t *testing.T) {
			t.Run("returns the backup id", func(t *testing.T) {
				idString := "68049826-6577-4763-97a7-c3f11a0f1b2f"
				id, err := resource.BackupIDFrom(idString)
				assert.NoError(t, err)
				assert.False(t, id.IsInvalid())
				assert.Equal(t, idString, id.String())
			})
		})
	})
	t.Run("Backup", func(t *testing.T) {
		store := resource.Bigquery
		tnnt, _ := tenant.NewTenant("project", "namespace")
		createdAt := time.Date(2022, 11, 18, 1, 0, 0, 0, time.UTC)

		t.Run("returns error when resource-names is empty", func(t *testing.T) {
			_, err := resource.NewBackup(store, tnnt, nil, "backup", time.Now(), map[string]string{})

			assert.Error(t, err)
			assert.EqualError(t, err, "invalid argument for entity backup: list of resources to backup is empty")
		})
		t.Run("returns error when one of resource names is empty", func(t *testing.T) {
			_, err := resource.NewBackup(store, tnnt, []string{"p.d.t", ""}, "backup", time.Now(), map[string]string{})

			assert.Error(t, err)
			assert.EqualError(t, err, "invalid argument for entity backup: one of resource names is empty")
		})
		t.Run("creates a backup object with properties", func(t *testing.T) {
			bk1, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "backup", createdAt, nil)

			assert.NoError(t, err)
			assert.Equal(t, "p.d.t", bk1.ResourceNames()[0])
			assert.Equal(t, store, bk1.Store())
			assert.Equal(t, tnnt, bk1.Tenant())
			assert.Equal(t, createdAt, bk1.CreatedAt())
			assert.Equal(t, "backup", bk1.Description())
			assert.Equal(t, 0, len(bk1.Config()))
		})
		t.Run("UpdateID", func(t *testing.T) {
			t.Run("returns error when backupID to update is nil", func(t *testing.T) {
				bk1, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "backup", createdAt, nil)
				assert.NoError(t, err)

				err = bk1.UpdateID(uuid.Nil)
				assert.Error(t, err)
				assert.EqualError(t, err, "invalid argument for entity backup: id to update is invalid")
			})
			t.Run("returns error when backup id already present", func(t *testing.T) {
				bk1, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "backup", createdAt, nil)
				assert.NoError(t, err)

				id1, err := uuid.Parse("12d8636a-fef8-4679-a853-82a5813802fd")
				assert.NoError(t, err)

				err = bk1.UpdateID(id1)
				assert.NoError(t, err)

				id2, err := uuid.Parse("c7a09492-7de3-4cdb-a983-28cccc167a2c")
				assert.NoError(t, err)

				err = bk1.UpdateID(id2)
				assert.Error(t, err)
				assert.EqualError(t, err, "invalid state for entity backup: trying to replace valid id 12d8636a-fef8-4679-a853-82a5813802fd")
			})
			t.Run("updates the backupID when not present", func(t *testing.T) {
				bk1, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "backup", createdAt, nil)
				assert.NoError(t, err)

				id1, err := uuid.Parse("12d8636a-fef8-4679-a853-82a5813802fd")
				assert.NoError(t, err)

				err = bk1.UpdateID(id1)
				assert.NoError(t, err)

				assert.Equal(t, id1.String(), bk1.ID().String())
			})
		})
		t.Run("GetConfigOrDefaultFor", func(t *testing.T) {
			t.Run("returns config value when present", func(t *testing.T) {
				conf := map[string]string{
					"dataset": "backup_optimus",
				}
				bk1, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "backup", createdAt, conf)
				assert.NoError(t, err)

				value := bk1.GetConfigOrDefaultFor("dataset", "fallback_value")
				assert.Equal(t, "backup_optimus", value)
			})
			t.Run("returns fallback value when not present", func(t *testing.T) {
				bk1, err := resource.NewBackup(store, tnnt, []string{"p.d.t"}, "backup", createdAt, nil)
				assert.NoError(t, err)

				value := bk1.GetConfigOrDefaultFor("dataset", "fallback_value")
				assert.Equal(t, "fallback_value", value)
			})
		})
	})
}
