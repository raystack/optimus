package local

type resourceSpecReadWriter struct {
	referenceFileName string
}

func NewResourceSpecReadWriter() (SpecReadWriter[*ResourceSpec], error) {
	return &resourceSpecReadWriter{
		referenceFileName: "resource.yaml",
	}, nil
}

func (r resourceSpecReadWriter) ReadAll(rootDirPath string) ([]*ResourceSpec, error) {
	dirPaths, err := discoverSpecDirPaths(rootDirPath, r.referenceFileName)
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

func (resourceSpecReadWriter) Write(path string, s *ResourceSpec) error {
	return nil
}

func (resourceSpecReadWriter) read(dirPath string) (*ResourceSpec, error) {
	return nil, nil
}
