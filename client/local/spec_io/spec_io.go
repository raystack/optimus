package spec_io

import (
	specModel "github.com/odpf/optimus/client/local/spec_model"
)

type ValidSpec interface {
	*specModel.JobSpec | *specModel.ResourceSpec
}

type SpecReader[S ValidSpec] interface {
	ReadAll(rootDirPath string) ([]S, error)
	ReadByName(rootDirPath, name string) (S, error)
}

type SpecWriter[S ValidSpec] interface {
	Write(path string, spec S) error
}

type SpecReadWriter[S ValidSpec] interface {
	SpecReader[S]
	SpecWriter[S]
}
