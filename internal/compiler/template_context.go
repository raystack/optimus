package compiler

import (
	"fmt"

	"github.com/odpf/optimus/internal/utils"
)

type ContextOpts struct {
	conf   map[string]string
	prefix string
	name   string
	append bool
}

func PrepareContext(builders ...ContextOpts) map[string]interface{} {
	contextMap := map[string]interface{}{}
	for _, b := range builders {
		if b.name != "" {
			contextMap[b.name] = b.conf
		}
		if b.prefix != "" {
			utils.AppendToMap(contextMap, prefixKeysOf(b.conf, b.prefix))
		}
		if b.append {
			utils.AppendToMap(contextMap, b.conf)
		}
	}
	return contextMap
}

func prefixKeysOf(configMap map[string]string, prefix string) map[string]string {
	prefixedConfig := map[string]string{}
	for key, val := range configMap {
		prefixedConfig[fmt.Sprintf("%s%s", prefix, key)] = val
	}
	return prefixedConfig
}

func From(confs ...map[string]string) ContextOpts {
	return ContextOpts{
		conf:   utils.MergeMaps(confs...),
		append: false,
	}
}

func (b ContextOpts) WithKeyPrefix(prefix string) ContextOpts {
	return ContextOpts{
		conf:   b.conf,
		name:   b.name,
		append: b.append,
		prefix: prefix,
	}
}

func (b ContextOpts) WithName(name string) ContextOpts {
	return ContextOpts{
		conf:   b.conf,
		prefix: b.prefix,
		append: b.append,
		name:   name,
	}
}

func (b ContextOpts) AddToContext() ContextOpts {
	return ContextOpts{
		conf:   b.conf,
		prefix: b.prefix,
		name:   b.name,
		append: true,
	}
}
