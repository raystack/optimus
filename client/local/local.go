package local

import (
	"io/fs"
	"path/filepath"
	"strings"
)

func discoverParentSpecFilePaths(specFS fs.FS, rootSpecDir, childSpecDir, referenceFileName string) ([]string, error) {
	return discoverPathsUsingSelector(specFS, rootSpecDir, func(path string, d fs.DirEntry) (string, bool) {
		if !strings.Contains(childSpecDir, filepath.Dir(path)) {
			return "", false
		}
		if !d.IsDir() && strings.HasSuffix(path, referenceFileName) {
			return path, true
		}
		return "", false
	})
}

func discoverSpecDirPaths(specFS fs.FS, rootSpecDir, referenceFileName string) ([]string, error) {
	return discoverPathsUsingSelector(specFS, rootSpecDir, func(path string, d fs.DirEntry) (string, bool) {
		if !d.IsDir() && strings.HasSuffix(path, referenceFileName) {
			return strings.TrimSuffix(path, referenceFileName), true
		}
		return "", false
	})
}

func discoverAssetFilePaths(fileFS fs.FS, rootDir string) ([]string, error) {
	return discoverPathsUsingSelector(fileFS, rootDir, func(path string, d fs.DirEntry) (string, bool) {
		if !d.IsDir() {
			return path, true
		}
		return "", false
	})
}

func discoverPathsUsingSelector(specFS fs.FS, rootSpecDir string, selectPath func(path string, d fs.DirEntry) (string, bool)) ([]string, error) {
	var specDirPaths []string
	err := fs.WalkDir(specFS, rootSpecDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if p, ok := selectPath(path, d); ok {
			specDirPaths = append(specDirPaths, p)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return specDirPaths, nil
}
