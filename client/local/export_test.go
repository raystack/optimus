package local

import "github.com/spf13/afero"

func NewTestResourceSpecReadWriter(specFS afero.Fs) SpecReadWriter[*ResourceSpec] {
	return &resourceSpecReadWriter{
		referenceSpecFileName: "resource.yaml",
		specFS:                specFS,
	}
}
