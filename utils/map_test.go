package utils_test

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/utils"
)

func TestMapHelper(t *testing.T) {
	t.Run("MergeAnyMaps", func(t *testing.T) {
		t.Run("clones the map when only one parameter", func(t *testing.T) {
			orig := map[string]interface{}{
				"Key": "Value",
			}

			clone := utils.MergeAnyMaps(orig)
			assert.NotNil(t, clone)
			assert.Equal(t, clone["Key"], orig["Key"])
			// Check if both map pointers are different
			assert.NotEqual(t, reflect.ValueOf(clone).Pointer(), reflect.ValueOf(orig).Pointer())
		})
		t.Run("merges when multiple maps", func(t *testing.T) {
			orig := map[string]interface{}{
				"Key1": "Value1",
				"Key2": "Value1",
			}
			orig2 := map[string]interface{}{
				"Key2": "Value2",
			}

			merged := utils.MergeAnyMaps(orig, orig2)
			assert.NotNil(t, merged)
			assert.Len(t, merged, 2)
			assert.Equal(t, merged["Key1"], orig["Key1"])
			assert.Equal(t, merged["Key2"], orig2["Key2"])
		})
	})
	t.Run("MergeMaps", func(t *testing.T) {
		t.Run("merges string maps", func(t *testing.T) {
			mp1 := map[string]string{
				"Key": "3",
			}
			mp2 := map[string]string{
				"Key2": "4",
			}

			merged := utils.MergeMaps(mp1, mp2)
			assert.NotNil(t, merged)
			assert.Equal(t, "3", merged["Key"])
			assert.Equal(t, "4", merged["Key2"])
		})
		t.Run("overrides the values in first map", func(t *testing.T) {
			mp1 := map[string]string{
				"Key": "3",
			}
			mp2 := map[string]string{
				"Key": "4",
			}

			merged := utils.MergeMaps(mp1, mp2)
			assert.NotNil(t, merged)
			assert.Equal(t, "4", merged["Key"])
		})
	})
	t.Run("AppendToMap", func(t *testing.T) {
		t.Run("appends data from string map", func(t *testing.T) {
			orig := map[string]interface{}{
				"Key": "Value1",
			}

			toAppend := map[string]string{
				"Key2": "Value2",
			}

			utils.AppendToMap(orig, toAppend)
			assert.Len(t, orig, 2)
			assert.Equal(t, "Value2", orig["Key2"])
		})
	})
}
