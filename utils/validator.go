package utils

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"

	"github.com/AlecAivazis/survey/v2"
	"github.com/robfig/cron/v3"
)

// CronIntervalValidator return a nil value when a valid cron string is passed
// used in gopkg.in/validator.v2
func CronIntervalValidator(val interface{}, _ string) error {
	value, ok := val.(string)
	if !ok {
		return fmt.Errorf("invalid crontab entry, not a valid string")
	}
	// an empty schedule is a valid schedule
	if value == "" {
		return nil
	}
	if _, err := cron.ParseStandard(value); err != nil {
		return fmt.Errorf("invalid crontab entry: %w", err)
	}
	return nil
}

// validatorFactory, name abbreviated so that
// the global implementation can be called 'validatorFactory'
type VFactory struct{}

func (*VFactory) NewFromRegex(re, message string) survey.Validator {
	regex := regexp.MustCompile(re)
	return func(v interface{}) error {
		k := reflect.ValueOf(v).Kind()
		if k != reflect.String {
			return fmt.Errorf("was expecting a string, got %s", k.String())
		}
		val, ok := v.(string)
		if !ok {
			return fmt.Errorf("error to cast [%+v] to string type", v)
		}
		matched := regex.MatchString(val)
		if matched == false {
			return errors.New(message)
		}
		return nil
	}
}

var ValidatorFactory = new(VFactory)

// ValidateCronInterval return a nil value when a valid cron string is passed
func ValidateCronInterval(val interface{}) error {
	return CronIntervalValidator(val, "")
}
