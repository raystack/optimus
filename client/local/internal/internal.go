package internal

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
	yamlV2 "gopkg.in/yaml.v2"
	"gopkg.in/yaml.v3"

	"github.com/odpf/optimus/client/local"
)

func DiscoverSpecDirPaths(specFS afero.Fs, rootSpecDir, referenceFileName string) ([]string, error) {
	return discoverPathsUsingSelector(specFS, rootSpecDir, func(path string, info fs.FileInfo) (string, bool) {
		if !info.IsDir() && strings.HasSuffix(path, referenceFileName) {
			return filepath.Dir(path), true
		}
		return "", false
	})
}

func DiscoverFilePaths(fileFS afero.Fs, rootDir string) ([]string, error) {
	return discoverPathsUsingSelector(fileFS, rootDir, func(path string, info fs.FileInfo) (string, bool) {
		if !info.IsDir() {
			return path, true
		}
		return "", false
	})
}

func discoverPathsUsingSelector(specFS afero.Fs, rootSpecDir string, selectPath func(path string, info fs.FileInfo) (string, bool)) ([]string, error) {
	var specDirPaths []string
	err := afero.Walk(specFS, rootSpecDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if p, ok := selectPath(path, info); ok {
			specDirPaths = append(specDirPaths, p)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return specDirPaths, nil
}

func WriteSpec[S local.ValidSpec](specFS afero.Fs, filePath string, spec S) error {
	if err := specFS.MkdirAll(filepath.Dir(filePath), os.ModePerm); err != nil {
		return err
	}
	fileSpec, err := specFS.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating spec under [%s]: %w", filePath, err)
	}
	indent := 2
	encoder := yaml.NewEncoder(fileSpec)
	encoder.SetIndent(indent)
	return encoder.Encode(spec)
}

func ReadSpec[S local.ValidSpec](specFS afero.Fs, filePath string) (S, error) {
	fileSpec, err := specFS.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening spec under [%s]: %w", filePath, err)
	}
	defer fileSpec.Close()

	var spec S
	if err := yamlV2.NewDecoder(fileSpec).Decode(&spec); err != nil {
		return nil, fmt.Errorf("error decoding spec under [%s]: %w", filePath, err)
	}
	return spec, nil
}

func GetFirstSpecByFilter[S local.ValidSpec](specs []S, filter func(S) bool) S {
	for _, s := range specs {
		if filter(s) {
			return s
		}
	}
	return nil
}
