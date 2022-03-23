package config

import (
	"errors"
	"fmt"
	"strings"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

// Validate validate the config as an input. If not valid, it returns error
func Validate(conf Config) error {
	switch c := conf.(type) {
	case ClientConfig:
		return validateClientConfig(c)
	case ServerConfig:
		return validateServerConfig(c)
	}
	return errors.New("error")
}

func validateClientConfig(conf ClientConfig) error {
	// implement this
	return validation.ValidateStruct(&conf,
		validation.Field(&conf.Version, validation.Required),
		validation.Field(&conf.Host, validation.Required),
		validation.Field(&conf.Log.Level, validation.In(
			LogLevelDebug,
			LogLevelInfo,
			LogLevelWarning,
			LogLevelError,
			LogLevelFatal,
		)),
		validation.Field(&conf.Namespaces, validation.By(validateNamespaces)),
		// ... etc
	)
}

func validateServerConfig(conf ServerConfig) error {
	// implement this
	return nil
}

func validateNamespaces(value interface{}) error {
	namespaces, ok := value.([]*Namespace)
	if !ok {
		return errors.New("error")
	}

	m := map[string]int{}
	for _, n := range namespaces {
		if n == nil {
			continue
		}
		m[n.Name]++
	}

	dup := []string{}
	for k, v := range m {
		if v > 1 {
			dup = append(dup, k)
		}
	}

	if len(dup) > 0 {
		return fmt.Errorf("duplicate namespaces are not allowed [%s]", strings.Join(dup, ","))
	}

	return nil
}
