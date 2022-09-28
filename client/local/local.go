package local

import (
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
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
