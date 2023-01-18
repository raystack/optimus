package compiler

import (
	"bytes"
	"strings"
	"text/template"
	"time"

	"github.com/odpf/optimus/internal/errors"
)

const (
	EntityCompiler = "compiler"

	// ISODateFormat https://en.wikipedia.org/wiki/ISO_8601
	ISODateFormat = "2006-01-02"

	ISOTimeFormat = time.RFC3339
)

// Engine compiles a set of defined macros using the provided context
type Engine struct {
	baseTemplate *template.Template
}

func NewEngine() *Engine {
	baseTemplate := template.
		New("optimus_template_engine").
		Funcs(OptimusFuncMap())

	return &Engine{
		baseTemplate: baseTemplate,
	}
}

func (e *Engine) Compile(templateMap map[string]string, context map[string]any) (map[string]string, error) {
	rendered := map[string]string{}

	for name, content := range templateMap {
		tmpl, err := e.baseTemplate.New(name).Parse(content)
		if err != nil {
			return nil, errors.InvalidArgument(EntityCompiler, "unable to parse content for "+name)
		}

		var buf bytes.Buffer
		err = tmpl.Execute(&buf, context)
		if err != nil {
			return nil, errors.InvalidArgument(EntityCompiler, "unable to render content for "+name)
		}
		rendered[name] = strings.TrimSpace(buf.String())
	}
	return rendered, nil
}

func (e *Engine) CompileString(input string, context map[string]any) (string, error) {
	tmpl, err := e.baseTemplate.New("base").Parse(input)
	if err != nil {
		return "", errors.InvalidArgument(EntityCompiler, "unable to parse string "+input)
	}
	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, context); err != nil {
		return "", errors.InvalidArgument(EntityCompiler, "unable to render string "+input)
	}
	return strings.TrimSpace(buf.String()), nil
}
