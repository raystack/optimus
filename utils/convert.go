package utils

import (
	"errors"
	"reflect"
	"strconv"

	"github.com/AlecAivazis/survey/v2"
)

func ConvertToStringMap(inputs map[string]interface{}) (map[string]string, error) {
	conv := map[string]string{}

	for key, val := range inputs {
		switch reflect.TypeOf(val).Name() {
		case "int":
			conv[key] = strconv.Itoa(val.(int))
		case "string":
			conv[key] = val.(string)
		case "OptionAnswer":
			conv[key] = val.(survey.OptionAnswer).Value
		default:
			return conv, errors.New("unknown type found while parsing user inputs")
		}
	}
	return conv, nil
}
