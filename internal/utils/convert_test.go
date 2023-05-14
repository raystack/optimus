package utils_test

import (
	"testing"

	"github.com/AlecAivazis/survey/v2"
	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/internal/utils"
)

func TestConvert(t *testing.T) {
	t.Run("convert map containing int, string and optionAnswer", func(t *testing.T) {
		optionAnswer := survey.OptionAnswer{
			Value: "value",
		}
		inputs := map[string]interface{}{
			"key-1": 1,
			"key-2": "string",
			"key-3": optionAnswer,
		}
		answerMap, err := utils.ConvertToStringMap(inputs)
		assert.Nil(t, err)
		assert.Equal(t, "1", answerMap["key-1"])
		assert.Equal(t, "string", answerMap["key-2"])
		assert.Equal(t, optionAnswer.Value, answerMap["key-3"])
	})
	t.Run("convert fails while converting double vals	", func(t *testing.T) {
		inputs := map[string]interface{}{
			"key-1": 0.5,
		}
		_, err := utils.ConvertToStringMap(inputs)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "unknown type found while parsing user inputs")
	})
}
