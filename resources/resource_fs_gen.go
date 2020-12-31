// +build ignore

package main

import (
	"log"
	"net/http"

	"github.com/shurcooL/vfsgen"
)

var resourceFS = http.Dir(".")

func main() {
	if err := vfsgen.Generate(resourceFS, vfsgen.Options{
		PackageName:  "resources",
		VariableName: "FileSystem",
		Filename:     "resource_fs.go",
	}); err != nil {
		log.Fatalln(err)
	}
}
