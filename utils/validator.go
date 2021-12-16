package utils

import (
	"fmt"
	"reflect"
	"regexp"

	"github.com/AlecAivazis/survey/v2"

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
	// an empty schedule is a valid schedule
	if value == "" {
		return nil
	}
	if _, err := cron.ParseStandard(value); err != nil {
		return errors.Wrap(err, "invalid crontab entry")
	}
	return nil
}

// validatorFactory, name abbreviated so that
// the global implementation can be called 'validatorFactory'
type VFactory struct{}

func (f *VFactory) NewFromRegex(re, message string) survey.Validator {
	var regex = regexp.MustCompile(re)
	return func(v interface{}) error {
		k := reflect.ValueOf(v).Kind()
		if k != reflect.String {
			return fmt.Errorf("was expecting a string, got %s", k.String())
		}
		val := v.(string)
		matched := regex.Match([]byte(val))
		if matched == false {
			return fmt.Errorf(message)
		}
		return nil
	}
}

var ValidatorFactory = new(VFactory)

// ValidateCronInterval return a nil value when a valid cron string is passed
func ValidateCronInterval(val interface{}) error {
	return CronIntervalValidator(val, "")
}
