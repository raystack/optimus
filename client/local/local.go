package local

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

func discoverSpecDirPaths(specFS afero.Fs, rootSpecDir, referenceFileName string) ([]string, error) {
	return discoverPathsUsingSelector(specFS, rootSpecDir, func(path string, info fs.FileInfo) (string, bool) {
		if !info.IsDir() && strings.HasSuffix(path, referenceFileName) {
			return filepath.Dir(path), true
		}
		return "", false
	})
}

func discoverFilePaths(fileFS afero.Fs, rootDir string) ([]string, error) {
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

func writeSpec[S ValidSpec](specFS afero.Fs, filePath string, spec S) error {
	fileSpec, err := specFS.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating spec under [%s]: %w", filePath, err)
	}
	return yaml.NewEncoder(fileSpec).Encode(spec)
}

func readSpec[S ValidSpec](specFS afero.Fs, filePath string) (S, error) {
	fileSpec, err := specFS.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening spec under [%s]: %w", filePath, err)
	}
	defer fileSpec.Close()

	var spec S
	if err := yaml.NewDecoder(fileSpec).Decode(&spec); err != nil {
		return nil, fmt.Errorf("error decoding spec under [%s]: %w", filePath, err)
	}
	return spec, nil
}

func getOne[S ValidSpec](specs []S, filter func(S) bool) S {
	for _, s := range specs {
		if filter(s) {
			return s
		}
	}
	return nil
}
