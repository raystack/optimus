package specio

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/spf13/afero"

	"github.com/raystack/optimus/client/local"
	"github.com/raystack/optimus/client/local/internal"
	"github.com/raystack/optimus/client/local/model"
)

type resourceSpecReadWriter struct {
	referenceSpecFileName string
	specFS                afero.Fs
}

func NewResourceSpecReadWriter(specFS afero.Fs) (local.SpecReadWriter[*model.ResourceSpec], error) {
	if specFS == nil {
		return nil, errors.New("spec fs is nil")
	}
	return &resourceSpecReadWriter{
		referenceSpecFileName: "resource.yaml",
		specFS:                specFS,
	}, nil
}

func (r resourceSpecReadWriter) ReadAll(rootDirPath string) ([]*model.ResourceSpec, error) {
	if rootDirPath == "" {
		return nil, errors.New("root dir path is empty")
	}

	specDirPaths, err := internal.DiscoverSpecDirPaths(r.specFS, rootDirPath, r.referenceSpecFileName)
	if err != nil {
		return nil, fmt.Errorf("error discovering spec paths under [%s]: %w", rootDirPath, err)
	}

	specs := make([]*model.ResourceSpec, len(specDirPaths))
	for i, dirPath := range specDirPaths {
		filePath := filepath.Join(dirPath, r.referenceSpecFileName)
		spec, err := internal.ReadSpec[*model.ResourceSpec](r.specFS, filePath)
		if err != nil {
			return nil, fmt.Errorf("error reading spec under [%s]: %w", filePath, err)
		}
		spec.Path = dirPath
		specs[i] = spec
	}
	return specs, nil
}

// TODO: in the future, we should make it so that we can identify the resource exist or not based on the file path
func (r resourceSpecReadWriter) ReadByName(rootDirPath, name string) (*model.ResourceSpec, error) {
	if name == "" {
		return nil, errors.New("name is empty")
	}
	allSpecs, err := r.ReadAll(rootDirPath)
	if err != nil {
		return nil, fmt.Errorf("error reading all specs under [%s]: %w", rootDirPath, err)
	}
	spec := internal.GetFirstSpecByFilter(allSpecs, func(rs *model.ResourceSpec) bool { return rs.Name == name })
	if spec == nil {
		return nil, fmt.Errorf("spec with name [%s] is not found", name)
	}
	return spec, nil
}

func (r resourceSpecReadWriter) Write(dirPath string, spec *model.ResourceSpec) error {
	if dirPath == "" {
		return errors.New("dir path is empty")
	}
	if spec == nil {
		return errors.New("spec is nil")
	}
	filePath := filepath.Join(dirPath, r.referenceSpecFileName)
	return internal.WriteSpec(r.specFS, filePath, spec)
}
