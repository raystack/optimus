package window

type cursorPointer string

const (
	pointToYear   cursorPointer = "year"
	pointToMonth  cursorPointer = "month"
	pointToDay    cursorPointer = "day"
	pointToHour   cursorPointer = "hour"
	pointToMinute cursorPointer = "minute"

	pointToTruncateTo cursorPointer = "truncate_to"
	pointToOffset     cursorPointer = "offset"
	pointToSize       cursorPointer = "size"
)

type truncateTo string

const (
	truncateToMonth truncateTo = "M"
	truncateToWeek  truncateTo = "w"
	truncateToDay   truncateTo = "d"
	truncateToHour  truncateTo = "h"
)
