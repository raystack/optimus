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
	v, ok := f.value[operand]
	if !ok {
		return ""
	}
	val, ok := v.(string)
	if !ok {
		return ""
	}
	return val
}

func (f *filter) GetStringArrayValue(operand Operand) []string {
	v, ok := f.value[operand]
	if !ok {
		return nil
	}
	val, ok := v.([]string)
	if !ok {
		return nil
	}
	return val
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
