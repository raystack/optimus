package dag

import (
	"strings"
	"text/template"
)

func OptimusFuncMap() template.FuncMap {
	return map[string]any{
		"replace":     Replace,
		"quote":       Quote,
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

func Trunc(c int, s string) string {
	if c >= 0 && len(s) > c {
		return s[:c]
	}
	return s
}
