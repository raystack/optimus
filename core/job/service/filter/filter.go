package filter

type FilterOpt func(*filter)

type filter struct {
	bits  uint64
	value map[Operand]string
}

func NewFilter(opts ...FilterOpt) *filter {
	f := &filter{}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

func (f *filter) GetValue(operand Operand) string {
	if v, ok := f.value[operand]; !ok {
		return ""
	} else {
		return v
	}
}
