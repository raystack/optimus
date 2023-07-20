package specio

import (
	"github.com/spf13/afero"

	"github.com/raystack/optimus/client/local"
	"github.com/raystack/optimus/client/local/model"
)

func NewTestJobSpecReadWriter(specFS afero.Fs) local.SpecReadWriter[*model.JobSpec] {
	return &jobSpecReadWriter{
		referenceSpecFileName:   "job.yaml",
		referenceParentFileName: "this.yaml",
		referenceAssetDirName:   "assets",
		specFS:                  specFS,
		withParentReading:       true,
	}
}

func NewTestResourceSpecReadWriter(specFS afero.Fs) local.SpecReadWriter[*model.ResourceSpec] {
	return &resourceSpecReadWriter{
		referenceSpecFileName: "resource.yaml",
		specFS:                specFS,
	}
}
