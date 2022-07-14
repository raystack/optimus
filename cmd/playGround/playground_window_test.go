package playground

import (
	"fmt"
	"testing"
	"time"

	"github.com/odpf/optimus/cmd/survey"
)

func TestTruncate(t *testing.T) {
	AssesrtCorrectDate := func(t testing.TB, got time.Time, want time.Time) {
		t.Helper()
		if got != want {
			t.Errorf("got %q want % q", got, want)
		}
	}

	surveyForInitilization := survey.WindowSurvey{}
	window := Window{surveyForInitilization}
	currentDate, _ := time.Parse("Jan 2, 2006 at 3:04pm (MST)", "Sep 21, 2002 at 6:30am (UTC)")
	expectedDate, _ := time.Parse("Jan 2, 2006 at 3:04pm (MST)", "Sep 21, 2002 at 6:00am (UTC)")
	t.Run("testing for 2002-09-21 6:30 , hourly", func(t *testing.T) {
		got := window.truncate(currentDate, "hour")
		want := expectedDate
		AssesrtCorrectDate(t, got, want)
	})
	expectedDate, _ = time.Parse("Jan 2, 2006 at 3:04pm (MST)", "Sep 21, 2002 at 12:00am (UTC)")
	t.Run("testing for 2002-09-21 6:30 , daily", func(t *testing.T) {
		got := window.truncate(currentDate, "day")
		want := expectedDate
		AssesrtCorrectDate(t, got, want)
	})
	expectedDate, _ = time.Parse("Jan 2, 2006 at 3:04pm (MST)", "Sep 16, 2002 at 12:00am (UTC)")
	t.Run("testing for 2002-09-21 6:30 , weekly", func(t *testing.T) {
		got := window.truncate(currentDate, "week")
		want := expectedDate
		AssesrtCorrectDate(t, got, want)
	})
	expectedDate, _ = time.Parse("Jan 2, 2006 at 3:04pm (MST)", "SEP 01, 2002 at 12:00am (UTC)")
	t.Run("testing for 2002-09-21 6:30 , monthy", func(t *testing.T) {
		got := window.truncate(currentDate, "month")
		want := expectedDate
		fmt.Println(want)
		AssesrtCorrectDate(t, got, want)
	})
}
func TestTruncateEdgeCases(t *testing.T) {
	surveyForInitilization := survey.WindowSurvey{}
	window := Window{surveyForInitilization}
	AssesrtCorrectDate := func(t testing.TB, got time.Time, want time.Time) {
		t.Helper()
		if got != want {
			t.Errorf("got %q want % q", got, want)
		}
	}
	currentDate, _ := time.Parse("Jan 2, 2006 at 3:04pm (MST)", "OCT 1, 2002 at 12:00am (UTC)")
	expectedDate, _ := time.Parse("Jan 2, 2006 at 3:04pm (MST)", "OCT 1, 2002 at 12:00am (UTC)")
	t.Run("testing for 2002-10-01 00:00 , hourly", func(t *testing.T) {
		got := window.truncate(currentDate, "hour")
		want := expectedDate
		AssesrtCorrectDate(t, got, want)
	})
	expectedDate, _ = time.Parse("Jan 2, 2006 at 3:04pm (MST)", "OCT 1, 2002 at 12:00am (UTC)")
	t.Run("testing for 2002-10-01 00:00 , daily", func(t *testing.T) {
		got := window.truncate(currentDate, "day")
		want := expectedDate
		AssesrtCorrectDate(t, got, want)
	})
	expectedDate, _ = time.Parse("Jan 2, 2006 at 3:04pm (MST)", "SEP 30, 2002 at 12:00am (UTC)")
	t.Run("testing for 2002-10-01 00:00 , weekly", func(t *testing.T) {
		got := window.truncate(currentDate, "week")
		want := expectedDate
		AssesrtCorrectDate(t, got, want)
	})
	expectedDate, _ = time.Parse("Jan 2, 2006 at 3:04pm (MST)", "OCT 1, 2002 at 12:00am (UTC)")
	t.Run("testing for 2002-10-01 00:00 , monthly", func(t *testing.T) {
		got := window.truncate(currentDate, "month")
		want := expectedDate
		AssesrtCorrectDate(t, got, want)
	})
	currentDate, _ = time.Parse("Jan 2, 2006 at 3:04pm (MST)", "Dec 31, 2002 at 11:42pm (UTC)")
	expectedDate, _ = time.Parse("Jan 2, 2006 at 3:04pm (MST)", "Dec 31, 2002 at 11:00pm (UTC)")
	t.Run("testing for 2002-12-31 23:42 , hourly", func(t *testing.T) {
		got := window.truncate(currentDate, "hour")
		want := expectedDate
		AssesrtCorrectDate(t, got, want)
	})
	expectedDate, _ = time.Parse("Jan 2, 2006 at 3:04pm (MST)", "Dec 31, 2002 at 12:00am (UTC)")
	t.Run("testing for 2002-12-31 23:42 , daily", func(t *testing.T) {
		got := window.truncate(currentDate, "day")
		want := expectedDate
		AssesrtCorrectDate(t, got, want)
	})
	expectedDate, _ = time.Parse("Jan 2, 2006 at 3:04pm (MST)", "Dec 1, 2002 at 12:00am (UTC)")
	t.Run("testing for 2002-12-31 23:42 , monthy", func(t *testing.T) {
		got := window.truncate(currentDate, "month")
		want := expectedDate
		AssesrtCorrectDate(t, got, want)
	})
	currentDate, _ = time.Parse("Jan 2, 2006 at 3:04pm (MST)", "Sep 23, 2002 at 12:00am (UTC)")
	expectedDate, _ = time.Parse("Jan 2, 2006 at 3:04pm (MST)", "Sep 23, 2002 at 12:00am (UTC)")
	t.Run("testing for 2002-09-23 00:00 , weekly", func(t *testing.T) {
		got := window.truncate(currentDate, "hour")
		want := expectedDate
		AssesrtCorrectDate(t, got, want)
	})

}
