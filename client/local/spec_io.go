package local

type ValidSpec interface {
	*JobSpec | *ResourceSpec
}

type SpecReader[S ValidSpec] interface {
	ReadAll(specRootDir string) ([]S, error)
}

type SpecWriter[S ValidSpec] interface {
	Write(path string, spec S) error
}

type SpecReadWriter[S ValidSpec] interface {
	SpecReader[S]
	SpecWriter[S]
}
