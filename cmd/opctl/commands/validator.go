package commands

import (
	"fmt"
	"reflect"
	"regexp"

	"gopkg.in/AlecAivazis/survey.v1"
	"github.com/odpf/optimus/utils"
)

// validatorFactory, name abbreviated so that
// the global implementation can be called 'validatorFactory'
type vFactory struct{}

func (f *vFactory) NewFromRegex(re, message string) survey.Validator {
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

var validatorFactory = new(vFactory)

// ValidateCronInterval return a nil value when a valid cron string is passed
func ValidateCronInterval(val interface{}) error {
	return utils.CronIntervalValidator(val, "")
}
