package playground

import (
	"strconv"
	"strings"
	"time"

	"github.com/odpf/optimus/job"
)

type state struct {
	windowv1      job.WindowV1
	windowv2      job.WindowV2
	sechduledDate time.Time
}

func (w *state) IncrementHour(duration string) string {
	splits := strings.SplitN(duration, "M", 2)
	hours, _ := time.ParseDuration(splits[1])
	new_hours := (hours.Hours() + 1)
	//fmt.Println(new_hours)
	return splits[0] + "M" + strconv.Itoa(int(new_hours)) + "h"
}
func (w *state) IncrementMonth(duration string) string {
	splits := strings.SplitN(duration, "M", 2)
	months, _ := strconv.Atoi(splits[0])
	months++
	return strconv.Itoa(months) + "M" + splits[1]
}
func (w *state) DecrementHour(duration string) string {
	splits := strings.SplitN(duration, "M", 2)
	hours, _ := time.ParseDuration(splits[1])
	new_hours := hours.Hours() - 1
	return splits[0] + "M" + strconv.Itoa(int(new_hours)) + "h"
}
func (w *state) DecrementMonth(duration string) string {
	splits := strings.SplitN(duration, "M", 2)
	months, _ := strconv.Atoi(splits[0])
	months--
	return strconv.Itoa(months) + "M" + splits[1]
}
func (w *state) IncrementDate(increaseBy string) time.Time {
	switch increaseBy {
	case "minute":
		return w.sechduledDate.Add(time.Minute)
	case "hour":
		return w.sechduledDate.Add(time.Hour)
	case "day":
		return w.sechduledDate.AddDate(0, 0, 1)
	case "month":
		return w.sechduledDate.AddDate(0, 1, 0)
	case "year":
		return w.sechduledDate.AddDate(1, 0, 0)
	}
	return w.sechduledDate
}
func (w *state) DecrementDate(decreaseBy string) time.Time {
	switch decreaseBy {
	case "minute":
		return w.sechduledDate.Add(-1 * time.Minute)
	case "hour":
		return w.sechduledDate.Add(-1 * time.Hour)
	case "day":
		return w.sechduledDate.AddDate(0, 0, -1)
	case "month":
		return w.sechduledDate.AddDate(0, -1, 0)
	case "year":
		return w.sechduledDate.AddDate(-1, 0, 0)
	}
	return w.sechduledDate
}
func (w *state) IncrementTruncate() string {
	switch w.windowv2.TruncateTo {
	case "":
		return "h"
	case "h":
		return "d"
	case "d":
		return "w"
	case "w":
		return "M"
	}
	return ""
}
func (w *state) DecrementTruncate() string {
	switch w.windowv2.TruncateTo {
	case "M":
		return "w"
	case "w":
		return "d"
	case "d":
		return "h"
	case "h":
		return ""
	}
	return ""
}
func (w *state) areWindowv1SizeAndOffsetValid() bool {
	splits := strings.SplitN(w.windowv2.Size, "M", 2)
	size_months, _ := strconv.Atoi(splits[0])
	splits = strings.SplitN(w.windowv2.Offset, "M", 2)
	offset_months, _ := strconv.Atoi(splits[0])
	return size_months == 0 && offset_months == 0
}
func (w *state) isWindowv1TruncateValid() bool {
	return w.windowv2.TruncateTo != ""
}
func (w *state) eliminateMonthFromSize(duration string) string {
	splits := strings.SplitN(duration, "M", 2)
	return splits[1]
}
func (w *state) getMonthsAndHours(duration string) (string, string) {
	months_string := strings.SplitN(duration, "M", 2)
	months_string_size := len(months_string[1])
	months_string[1] = months_string[1][:months_string_size-1]
	return months_string[0], months_string[1]
}
func (w *state) genarateV1TimeRange() string {
	if !w.areWindowv1SizeAndOffsetValid() {
		return "version1 does not support the size , offset to be in months\n"
	}
	if !w.isWindowv1TruncateValid() {
		return "version 1 does not support truncate to be none\n"
	}
	s := ""
	w.windowv1.Size = w.eliminateMonthFromSize(w.windowv2.Size)
	w.windowv1.Offset = w.eliminateMonthFromSize(w.windowv2.Offset)
	w.windowv1.TruncateTo = w.windowv2.TruncateTo
	dstartv1, dendv1, _ := w.windowv1.GetTimeRange(w.sechduledDate)
	s += "dstart v1 : " + dstartv1.Format("2006-01-02 15:04:05") + "     dend v1 :" + dendv1.Format("2006-01-02 15:04:05")
	s += "\n"
	return s
}
func (w *state) genarateV2TimeRange() string {
	s := ""
	dstartv2, dendv2, _ := w.windowv2.GetTimeRange(w.sechduledDate)
	s += "dstart v2 : " + dstartv2.Format("2006-01-02 15:04:05") + "     dend v2 :" + dendv2.Format("2006-01-02 15:04:05") + "\n"
	return s
}
