package models

func NewTestWindowV1(truncateTo, offset, size string) Window {
	return &WindowV1{
		TruncateTo: truncateTo,
		Offset:     offset,
		Size:       size,
	}
}

func NewTestWindowV2(truncateTo, offset, size string) Window {
	return WindowV2{
		TruncateTo: truncateTo,
		Offset:     offset,
		Size:       size,
	}
}
