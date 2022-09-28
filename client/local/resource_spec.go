package local

import (
	"github.com/spf13/afero"
)

type resourceSpecReadWriter struct {
	referenceFileName string
	specFS            afero.Fs
}

func NewResourceSpecReadWriter(specFS afero.Fs) (SpecReadWriter[*ResourceSpec], error) {
	return &resourceSpecReadWriter{
		referenceFileName: "resource.yaml",
		specFS:            specFS,
	}, nil
}

func (r *resourceSpecReadWriter) ReadAll(rootDirPath string) ([]*ResourceSpec, error) {
	dirPaths, err := discoverSpecDirPaths(r.specFS, rootDirPath, r.referenceFileName)
	if err != nil {
		return nil, err
	}
	var output []*ResourceSpec
	for _, p := range dirPaths {
		spec, err := r.read(p)
		if err != nil {
			return nil, err
		}
		output = append(output, spec)
	}
	return output, nil
}

func (*resourceSpecReadWriter) Write(dirPath string, spec *ResourceSpec) error {
	// TODO: implement write resource spec here. Given dirPath and resource spec
	// create resource.yaml specification inside dirPath folder
	return nil
}

func (*resourceSpecReadWriter) read(dirPath string) (*ResourceSpec, error) {
	// TODO: implement read 1 resource spec given their dirPath
	return nil, nil
}
