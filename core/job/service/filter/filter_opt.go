package filter

type FilterOpt func(*filter)
type Operand uint64

const (
	bitOnProjectName         uint64 = 1 << 0
	bitOnJobName             uint64 = 1 << 1
	bitOnResourceDestination uint64 = 1 << 2
)

const (
	ProjectName         = Operand(bitOnProjectName)
	JobName             = Operand(bitOnJobName)
	ResourceDestination = Operand(bitOnResourceDestination)
)

func With(operand Operand, value string) FilterOpt {
	return func(f *filter) {
		if value != "" {
			f.bits |= uint64(operand)
			f.value[operand] = value
		}
	}
}
