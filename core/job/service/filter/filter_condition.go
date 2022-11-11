package filter

func (f *filter) Contains(operands ...Operand) bool {
	for _, operand := range operands {
		if (f.bits & uint64(operand)) == uint64(0) {
			return false
		}
	}
	return true
}
