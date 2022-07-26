package models_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/models"
)

func TestWindowV1(t *testing.T) {
	t.Run("Validate", func(t *testing.T) {
		t.Run("should not return error for window size which is not a positive or an instant time duration", func(t *testing.T) {
			validWindowConfigs := []string{"24h", "2h45m", "60s", "45m24h", "", "0"}
			for _, config := range validWindowConfigs {
				window, err := models.NewWindow(1, "", "", config)
				if err != nil {
					panic(err)
				}
				err = window.Validate()
				assert.Nil(t, err, fmt.Sprintf("failed for : %s", config))
			}
		})
		t.Run("should not return error for window offset which is a valid time duration", func(t *testing.T) {
			validOffsetConfigs := []string{"24h", "2h45m", "60s", "45m24h", "0", ""}
			for _, config := range validOffsetConfigs {
				window, err := models.NewWindow(1, "", config, "")
				if err != nil {
					panic(err)
				}
				err = window.Validate()
				assert.Nil(t, err, fmt.Sprintf("failed for : %s", config))
			}
		})
		t.Run("should not return error for valid window truncate configs", func(t *testing.T) {
			validTruncateConfigs := []string{"h", "d", "w", "m", "M", ""}
			for _, config := range validTruncateConfigs {
				window, err := models.NewWindow(1, config, "", "")
				if err != nil {
					panic(err)
				}
				err = window.Validate()
				assert.Nil(t, err, fmt.Sprintf("failed for : %s", config))
			}
		})
	})
}
