package local

import (
	"io/fs"
	"strings"
)

func discoverSpecDirPaths(specFS fs.FS, rootSpecDir, referenceFileName string) ([]string, error) {
	var specDirPaths []string
	err := fs.WalkDir(specFS, rootSpecDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, referenceFileName) {
			specDirPaths = append(specDirPaths, strings.TrimSuffix(path, referenceFileName))
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	return specDirPaths, nil
}
