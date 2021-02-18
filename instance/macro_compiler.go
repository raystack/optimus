package instance

import (
	"bytes"
	"strings"
	"text/template"
)

// MacroCompiler compiles a set of defined macros using the provided values
type MacroCompiler struct {
}

func NewMacroCompiler() *MacroCompiler {
	return &MacroCompiler{}
}

func (c *MacroCompiler) CompileTemplate(values map[string]string, input string) (string, error) {
	tmpl, err := template.New("MacroCompiler").Parse(input)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, values); err != nil {
		return "", err
	}
	return strings.TrimSpace(buf.String()), nil
}
