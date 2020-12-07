package utils

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
)

// CronIntervalValidator return a nil value when a valid cron string is passed
// used in gopkg.in/validator.v2
func CronIntervalValidator(val interface{}, param string) error {
	value, ok := val.(string)
	if !ok {
		return fmt.Errorf("invalid crontab entry, not a valid string")
	}
	if _, err := cron.ParseStandard(value); err != nil {
		return errors.Wrap(err, "invalid crontab entry")
	}
	return nil
}
