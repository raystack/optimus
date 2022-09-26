package local

import (
	"io/fs"
)

type jobSpecReadWriter struct {
	referenceFileName string
	specFS            fs.FS
}

func NewJobSpecReadWriter(specFS fs.FS) (SpecReadWriter[*JobSpec], error) {
	return &jobSpecReadWriter{
		referenceFileName: "job.yaml",
		specFS:            specFS,
	}, nil
}

func (j *jobSpecReadWriter) ReadAll(rootDirPath string) ([]*JobSpec, error) {
	dirPaths, err := discoverSpecDirPaths(j.specFS, rootDirPath, j.referenceFileName)
	if err != nil {
		return nil, err
	}
	var output []*JobSpec
	for _, p := range dirPaths {
		spec, err := j.read(p)
		if err != nil {
			return nil, err
		}
		output = append(output, spec)
	}
	return output, nil
}

func (*jobSpecReadWriter) Write(dirPath string, spec *JobSpec) error {
	// TODO: implement write job spec here. Given dirPath and job spec
	// create job.yaml specification as well as their asset inside dirPath folder
	return nil
}

func (*jobSpecReadWriter) read(dirPath string) (*JobSpec, error) {
	// TODO: implement read 1 job spec given their dirPath
	return nil, nil
}
