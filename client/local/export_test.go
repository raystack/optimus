package local

import "github.com/spf13/afero"

func NewTestJobSpecReadWriter(specFS afero.Fs) SpecReadWriter[*JobSpec] {
	return &jobSpecReadWriter{
		referenceSpecFileName:   "job.yaml",
		referenceParentFileName: "this.yaml",
		referenceAssetDirName:   "assets",
		specFS:                  specFS,
	}
}

func NewTestResourceSpecReadWriter(specFS afero.Fs) SpecReadWriter[*ResourceSpec] {
	return &resourceSpecReadWriter{
		referenceSpecFileName: "resource.yaml",
		specFS:                specFS,
	}
}
