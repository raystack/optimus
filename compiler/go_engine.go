package compiler

import (
	"bytes"
	"strings"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	"github.com/odpf/optimus/models"
)

// GoEngine compiles a set of defined macros using the provided context
type GoEngine struct {
	baseFns template.FuncMap
}

func NewGoEngine() *GoEngine {
	e := &GoEngine{}
	e.init()
	return e
}

func (e *GoEngine) CompileFiles(files map[string]string, context map[string]interface{}) (map[string]string, error) {
	var err error
	rendered := map[string]string{}
	// prepare template list
	root := template.New("base").Funcs(e.baseFns)
	for name, content := range files {
		root, err = root.New(name).Parse(content)
		if err != nil {
			return nil, err
		}
	}
	// render templates
	for name, content := range files {
		// don't render files starting with
		if shouldIgnoreFile(name) {
			rendered[name] = content
			continue
		}
		var buf bytes.Buffer
		err = root.ExecuteTemplate(&buf, name, context)
		if err != nil {
			return nil, err
		}
		rendered[name] = buf.String()
	}
	return rendered, nil
}

func (e *GoEngine) CompileString(input string, context map[string]interface{}) (string, error) {
	tmpl, err := template.New("optimus_go_engine").Funcs(e.baseFns).Parse(input)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, context); err != nil {
		return "", err
	}
	return strings.TrimSpace(buf.String()), nil
}

func shouldIgnoreFile(name string) bool {
	for _, ext := range IgnoreTemplateRenderExtension {
		if strings.HasSuffix(name, ext) {
			return true
		}
	}
	return false
}

func (e *GoEngine) init() {
	e.baseFns = sprig.TxtFuncMap()
	e.baseFns["Date"] = goDateFn
}

func goDateFn(timeStr string) (string, error) {
	t, err := time.Parse(models.InstanceScheduledAtTimeLayout, timeStr)
	if err != nil {
		return "", err
	}
	return t.Format(models.JobDatetimeLayout), nil
}
