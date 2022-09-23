package local

type jobSpecReadWriter struct {
	referenceFileName string
}

func NewJobSpecReadWriter() (SpecReadWriter[*JobSpec], error) {
	return &jobSpecReadWriter{
		referenceFileName: "job.yaml",
	}, nil
}

func (j jobSpecReadWriter) ReadAll(rootDirPath string) ([]*JobSpec, error) {
	dirPaths, err := discoverSpecDirPaths(rootDirPath, j.referenceFileName)
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

func (jobSpecReadWriter) Write(path string, s *JobSpec) error {
	return nil
}

func (jobSpecReadWriter) read(dirPath string) (*JobSpec, error) {
	return nil, nil
}
