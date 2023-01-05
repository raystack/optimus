package plugin

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/AlecAivazis/survey/v2"
)

type Options struct {
	DryRun bool
}

// USED in Question Validations
type vFactory struct{}

func (*vFactory) NewFromRegex(re, message string) survey.Validator {
	var regex = regexp.MustCompile(re)
	return func(v interface{}) error {
		k := reflect.ValueOf(v).Kind()
		if k != reflect.String {
			return fmt.Errorf("was expecting a string, got %s", k.String())
		}
		val := v.(string)
		if !regex.Match([]byte(val)) {
			return fmt.Errorf("%s", message)
		}
		return nil
	}
}

var ValidatorFactory = new(vFactory)

type Question struct {
	Name        string   `yaml:",omitempty"`
	Prompt      string   `yaml:",omitempty"`
	Help        string   `yaml:",omitempty"`
	Default     string   `yaml:",omitempty"`
	Multiselect []string `yaml:",omitempty"`

	SubQuestions []SubQuestion `yaml:",omitempty"`

	Regexp          string `yaml:",omitempty"`
	ValidationError string `yaml:",omitempty"`
	MinLength       int    `yaml:",omitempty"`
	MaxLength       int    `yaml:",omitempty"`
	Required        bool   `yaml:",omitempty"`
}

func (q *Question) IsValid(value string) error {
	if q.Required {
		return survey.Required(value)
	}
	var validators []survey.Validator
	if q.Regexp != "" {
		validators = append(validators, ValidatorFactory.NewFromRegex(q.Regexp, q.ValidationError))
	}
	if q.MinLength != 0 {
		validators = append(validators, survey.MinLength(q.MinLength))
	}
	if q.MaxLength != 0 {
		validators = append(validators, survey.MaxLength(q.MaxLength))
	}
	return survey.ComposeValidators(validators...)(value)
}

type SubQuestion struct {
	// IfValue is used as an if condition to match with user input
	// if user value matches this only then ask sub questions
	IfValue   string
	Questions Questions
}

type Questions []Question

func (q Questions) Get(name string) (Question, bool) {
	for _, que := range q {
		if strings.EqualFold(que.Name, name) {
			return que, true
		}
	}
	return Question{}, false
}

type Answer struct {
	Question Question
	Value    string
}

type Answers []Answer

func (ans Answers) Get(name string) (Answer, bool) {
	for _, a := range ans {
		if strings.EqualFold(a.Question.Name, name) {
			return a, true
		}
	}
	return Answer{}, false
}
