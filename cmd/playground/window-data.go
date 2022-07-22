package playground

type cursorPointer string

const (
	year            cursorPointer = "year"
	month           cursorPointer = "month"
	minute          cursorPointer = "minute"
	day             cursorPointer = "day"
	hour            cursorPointer = "hour"
	defaultSize     cursorPointer = "0M0h"
	defaultTruncate cursorPointer = "h"
	size            cursorPointer = "size"
	offset          cursorPointer = "offset"
	truncateTo      cursorPointer = "truncateTo"
)

func (c cursorPointer) getStringValue() string {
	return string(c)
}
