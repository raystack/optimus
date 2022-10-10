package specio

type jobSpecReadWriterOpt func(*jobSpecReadWriter) error

func WithJobSpecParentReading() jobSpecReadWriterOpt {
	return func(j *jobSpecReadWriter) error {
		j.withParentReading = true
		return nil
	}
}
