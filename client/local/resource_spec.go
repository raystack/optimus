package local

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/spf13/afero"
)

type resourceSpecReadWriter struct {
	referenceSpecFileName string
	specFS                afero.Fs
}

func NewResourceSpecReadWriter(specFS afero.Fs) (SpecReadWriter[*ResourceSpec], error) {
	if specFS == nil {
		return nil, errors.New("spec fs is nil")
	}
	return &resourceSpecReadWriter{
		referenceSpecFileName: "resource.yaml",
		specFS:                specFS,
	}, nil
}

func (r resourceSpecReadWriter) ReadAll(rootDirPath string) ([]*ResourceSpec, error) {
	if rootDirPath == "" {
		return nil, errors.New("root dir path is empty")
	}

	specDirPaths, err := discoverSpecDirPaths(r.specFS, rootDirPath, r.referenceSpecFileName)
	if err != nil {
		return nil, fmt.Errorf("error discovering spec paths under [%s]: %w", rootDirPath, err)
	}

	specs := make([]*ResourceSpec, len(specDirPaths))
	for i, dirPath := range specDirPaths {
		filePath := filepath.Join(dirPath, r.referenceSpecFileName)
		spec, err := readSpec[*ResourceSpec](r.specFS, filePath)
		if err != nil {
			return nil, fmt.Errorf("error reading spec under [%s]: %w", filePath, err)
		}
		specs[i] = spec
	}
	return specs, nil
}

func (r resourceSpecReadWriter) ReadByName(rootDirPath, name string) (*ResourceSpec, error) {
	if name == "" {
		return nil, errors.New("name is empty")
	}
	allSpecs, err := r.ReadAll(rootDirPath)
	if err != nil {
		return nil, fmt.Errorf("error reading all specs under [%s]: %w", rootDirPath, err)
	}
	spec := getOne(allSpecs, func(rs *ResourceSpec) bool { return rs.Name == name })
	if spec == nil {
		return nil, fmt.Errorf("spec with name [%s] is not found", name)
	}
	return spec, nil
}

func (r resourceSpecReadWriter) Write(dirPath string, spec *ResourceSpec) error {
	if dirPath == "" {
		return errors.New("dir path is empty")
	}
	if spec == nil {
		return errors.New("spec is nil")
	}
	filePath := filepath.Join(dirPath, r.referenceSpecFileName)
	return writeSpec(r.specFS, filePath, spec)
}
