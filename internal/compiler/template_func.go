package compiler

import (
	"strconv"
	"strings"
	"text/template"
	"time"
)

func OptimusFuncMap() template.FuncMap {
	return map[string]any{
		"Date":        Date,
		"replace":     Replace,
		"trunc":       Trunc,
		"date":        date,
		"date_modify": DateModify,
		"toDate":      toDate,
		"unixEpoch":   UnixEpoch,
		"list":        List,
		"join":        Join,
	}
}

func Date(timeStr string) (string, error) {
	t, err := time.Parse(ISOTimeFormat, timeStr)
	if err != nil {
		return "", err
	}
	return t.Format(ISODateFormat), nil
}

func Replace(old, newStr, name string) string {
	return strings.ReplaceAll(name, old, newStr)
}

func Trunc(c int, s string) string {
	if c >= 0 && len(s) > c {
		return s[:c]
	}
	return s
}

func date(fmt string, date interface{}) string {
	// Cannot have a reliable test, depends on local machine time
	return dateInZone(fmt, date, "Local")
}

func dateInZone(fmt string, date interface{}, zone string) string {
	var t time.Time
	switch date := date.(type) {
	default:
		t = time.Now()
	case time.Time:
		t = date
	case *time.Time:
		t = *date
	case int64:
		t = time.Unix(date, 0)
	case int:
		t = time.Unix(int64(date), 0)
	case int32:
		t = time.Unix(int64(date), 0)
	}

	loc, err := time.LoadLocation(zone)
	if err != nil {
		loc, _ = time.LoadLocation("UTC")
	}

	return t.In(loc).Format(fmt)
}

func DateModify(fmt string, date time.Time) time.Time {
	d, err := time.ParseDuration(fmt)
	if err != nil {
		return date
	}
	return date.Add(d)
}

func toDate(fmt, str string) time.Time {
	// Cannot have a reliable test, depends on local machine time
	t, _ := time.ParseInLocation(fmt, str, time.Local)
	return t
}

func UnixEpoch(date time.Time) string {
	return strconv.FormatInt(date.Unix(), 10) //nolint
}

func List(v ...string) []string {
	return v
}

func Join(sep string, v []string) string {
	return strings.Join(v, sep)
}
