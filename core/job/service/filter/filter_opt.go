package filter

type FilterOpt func(*filter)
type Operand uint64

const (
	bitOnProjectName         uint64 = 1 << 0
	bitOnJobName             uint64 = 1 << 1
	bitOnResourceDestination uint64 = 1 << 2
	bitOnNamespaceNames      uint64 = 1 << 3
	bitOnJobNames            uint64 = 1 << 4
)

const (
	ProjectName         = Operand(bitOnProjectName)
	NamespaceNames      = Operand(bitOnNamespaceNames)
	JobName             = Operand(bitOnJobName)
	JobNames            = Operand(bitOnJobNames)
	ResourceDestination = Operand(bitOnResourceDestination)
)

func With(operand Operand, value interface{}) FilterOpt {
	return func(f *filter) {
		if value != "" {
			f.bits |= uint64(operand)
			f.value[operand] = value
		}
	}
}
