package filter

type filter struct {
	bits  uint64
	value map[Operand]interface{}
}

func NewFilter(opts ...FilterOpt) *filter {
	f := &filter{
		bits:  0,
		value: make(map[Operand]interface{}),
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

func (f *filter) GetStringValue(operand Operand) string {
	if v, ok := f.value[operand]; !ok {
		return ""
	} else {
		return v.(string)
	}
}

func (f *filter) GetStringArrayValue(operand Operand) []string {
	if v, ok := f.value[operand]; !ok {
		return nil
	} else {
		return v.([]string)
	}
}

// Contains provide conditional check for the filter if all operands satisfied by the filter.
func (f *filter) Contains(operands ...Operand) bool {
	for _, operand := range operands {
		if (f.bits & uint64(operand)) == uint64(0) {
			return false
		}
	}
	return true
}
