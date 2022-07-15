package models

func NewTestWindowV1(truncateTo, offset, size string) Window {
	return windowV1{
		truncateTo: truncateTo,
		offset:     offset,
		size:       size,
	}
}

func NewTestWindowV2(truncateTo, offset, size string) Window {
	return windowV2{
		truncateTo: truncateTo,
		offset:     offset,
		size:       size,
	}
}
