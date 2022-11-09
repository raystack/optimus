package dag

import (
	"reflect"
	"strings"
	"text/template"
)

func OptimusFuncMap() template.FuncMap {
	return map[string]any{
		"replace":     Replace,
		"quote":       Quote,
		"empty":       empty,
		"trunc":       Trunc,
		"ReplaceDash": ReplaceDash,
		"DisplayName": DisplayName,
	}
}

func ReplaceDash(name string) string {
	return strings.ReplaceAll(name, "-", "__dash__")
}

func DisplayName(name string) string {
	return strings.ReplaceAll(ReplaceDash(name), ".", "__dot__")
}

func Replace(old, new, name string) string {
	return strings.ReplaceAll(name, old, new)
}

func Quote(str string) string {
	return `"` + str + `"`
}

// TODO: remove, check for nil in resources
func empty(given interface{}) bool {
	g := reflect.ValueOf(given)
	if !g.IsValid() {
		return true
	}

	// Basically adapted from text/template.isTrue
	switch g.Kind() {
	default:
		return g.IsNil()
	case reflect.Array, reflect.Slice, reflect.Map, reflect.String:
		return g.Len() == 0
	case reflect.Bool:
		return !g.Bool()
	case reflect.Complex64, reflect.Complex128:
		return g.Complex() == 0
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return g.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return g.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return g.Float() == 0
	case reflect.Struct:
		return false
	}
}

func Trunc(c int, s string) string {
	if c >= 0 && len(s) > c {
		return s[:c]
	}
	return s
}
