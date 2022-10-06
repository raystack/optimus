package spec_io

import (
	"github.com/spf13/afero"

	specModel "github.com/odpf/optimus/client/local/spec_model"
)

func NewTestJobSpecReadWriter(specFS afero.Fs) SpecReadWriter[*specModel.JobSpec] {
	return &jobSpecReadWriter{
		referenceSpecFileName:   "job.yaml",
		referenceParentFileName: "this.yaml",
		referenceAssetDirName:   "assets",
		specFS:                  specFS,
	}
}

func NewTestResourceSpecReadWriter(specFS afero.Fs) SpecReadWriter[*specModel.ResourceSpec] {
	return &resourceSpecReadWriter{
		referenceSpecFileName: "resource.yaml",
		specFS:                specFS,
	}
}
