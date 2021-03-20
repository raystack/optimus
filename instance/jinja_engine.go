package instance

import (
	"bytes"
	"io"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/flosch/pongo2"
	"github.com/odpf/optimus/models"
)

// JinjaEngine compiles a set of defined macros using the provided context
type JinjaEngine struct {
}

func NewJinjaEngine() *JinjaEngine {
	return &JinjaEngine{}
}

func (c *JinjaEngine) CompileFiles(files map[string]string, context map[string]interface{}) (map[string]string, error) {
	rendered := map[string]string{}
	templateSet := pongo2.NewSet("inmemory", NewInMemoryTemplateLoader(files))
	for name, content := range files {
		// don't render files starting with
		if strings.HasSuffix(name, IgnoreTemplateRenderExtension) {
			rendered[name] = content
			continue
		}

		tpl, err := templateSet.FromString(content)
		if err != nil {
			return nil, err
		}
		parsed, err := tpl.Execute(context)
		if err != nil {
			return nil, err
		}
		rendered[name] = parsed
	}
	return rendered, nil
}

func (c *JinjaEngine) CompileString(input string, context map[string]interface{}) (string, error) {
	tpl, err := pongo2.FromString(input)
	if err != nil {
		return "", err
	}
	return tpl.Execute(context)
}

type inMemoryTemplateLoader struct {
	files map[string]string
}

//NewInMemoryTemplateLoader emulates map of string as files for templates
func NewInMemoryTemplateLoader(files map[string]string) *inMemoryTemplateLoader {
	return &inMemoryTemplateLoader{
		files: files,
	}
}

func (loader inMemoryTemplateLoader) Abs(base, name string) string {
	return name
}

func (loader inMemoryTemplateLoader) Get(path string) (io.Reader, error) {
	data, ok := loader.files[path]
	if !ok {
		return nil, errors.Errorf("file not found: %s", path)
	}
	return bytes.NewReader([]byte(data)), nil
}

func init() {
	pongo2.RegisterFilter("ToDate", func(in *pongo2.Value, param *pongo2.Value) (*pongo2.Value, *pongo2.Error) {
		t, err := time.Parse(models.InstanceScheduledAtTimeLayout, in.String())
		if err != nil {
			return nil, &pongo2.Error{
				Sender:    "filter:ToDate",
				OrigError: err,
			}
		}
		return pongo2.AsValue(t.Format(models.JobDatetimeLayout)), nil
	})
	pongo2.RegisterTag("list", tagListParser)
}

type tagListNode struct {
	name string
	data []interface{}
}

func (node *tagListNode) Execute(ctx *pongo2.ExecutionContext, writer pongo2.TemplateWriter) *pongo2.Error {
	ctx.Private[node.name] = node.data
	return nil
}

func tagListParser(doc *pongo2.Parser, start *pongo2.Token, arguments *pongo2.Parser) (pongo2.INodeTag, *pongo2.Error) {
	node := &tagListNode{}

	// Parse variable name
	typeToken := arguments.MatchType(pongo2.TokenIdentifier)
	if typeToken == nil {
		return nil, arguments.Error("Expected an identifier.", nil)
	}
	node.name = typeToken.Val

	if arguments.Match(pongo2.TokenSymbol, "=") == nil {
		return nil, arguments.Error("Expected '='.", nil)
	}

	// Variable expression
	listSize := arguments.Remaining()
	node.data = make([]interface{}, 0)

	for idx := 0; idx < listSize; idx++ {
		nextToken := arguments.MatchType(pongo2.TokenString)
		if nextToken == nil {
			nextToken = arguments.MatchType(pongo2.TokenNumber)
		}
		if nextToken == nil {
			return nil, arguments.Error("Expected an string.", nil)
		}
		node.data = append(node.data, nextToken.Val)
	}

	// Remaining arguments
	if arguments.Remaining() > 0 {
		return nil, arguments.Error("Malformed 'list'-tag arguments.", nil)
	}
	return node, nil
}
