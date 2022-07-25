package playground

const (
	defaultSize     string = "0M0h"
	defaultTruncate string = "h"
)

type cursorPointer string

const (
	pointToYear     cursorPointer = "year"
	pointToMonth    cursorPointer = "month"
	pointToMinute   cursorPointer = "minute"
	PointToDay      cursorPointer = "day"
	pointToHour     cursorPointer = "hour"
	pointToSize     cursorPointer = "size"
	pointToOffset   cursorPointer = "offset"
	pointToTruncate cursorPointer = "truncateTo"
)

func (c cursorPointer) getStringValue() string {
	return string(c)
}
