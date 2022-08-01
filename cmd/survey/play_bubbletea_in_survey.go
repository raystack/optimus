package survey

import (
	"fmt"
	"reflect"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

var (
	day    = "day"
	month  = "month"
	year   = "year"
	minute = "minute"
	hour   = "hour"
)

type TimeSurvey struct {
	CurrentTime time.Time
	cursor      string
}

func GetTimeSurvey() *TimeSurvey {
	return &TimeSurvey{time.Now(), day}
}
func (m *TimeSurvey) Init() tea.Cmd {
	return nil
}
func (m *TimeSurvey) handle_up() {
	switch m.cursor {
	case day:
		m.CurrentTime = m.CurrentTime.AddDate(0, 0, 1)
	case month:
		m.CurrentTime = m.CurrentTime.AddDate(0, 1, 0)
	case year:
		m.CurrentTime = m.CurrentTime.AddDate(1, 0, 0)
	case hour:
		m.CurrentTime = m.CurrentTime.Add(time.Hour)
	case minute:
		m.CurrentTime = m.CurrentTime.Add(time.Minute)
	}
}
func (m *TimeSurvey) handle_down() {
	switch m.cursor {
	case day:
		m.CurrentTime = m.CurrentTime.AddDate(0, 0, -1)
	case month:
		m.CurrentTime = m.CurrentTime.AddDate(0, -1, 0)
	case year:
		m.CurrentTime = m.CurrentTime.AddDate(-1, 0, 0)
	case hour:
		m.CurrentTime = m.CurrentTime.Add(-1 * time.Hour)
	case minute:
		m.CurrentTime = m.CurrentTime.Add(-1 * time.Minute)
	}
}
func (m *TimeSurvey) handle_left() {
	switch m.cursor {
	case month:
		m.cursor = day
	case year:
		m.cursor = month
	case hour:
		m.cursor = year
	case minute:
		m.cursor = hour
	}
}
func (m *TimeSurvey) handle_right() {
	switch m.cursor {
	case hour:
		m.cursor = minute
	case year:
		m.cursor = hour
	case month:
		m.cursor = year
	case day:
		m.cursor = month
	}
}
func (m *TimeSurvey) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	currMsg := reflect.TypeOf(msg)
	if currMsg.String() != "tea.KeyMsg" {
		return m, nil
	}
	switch fmt.Sprintf("%s", msg) {
	case "ctrl+c", "q":
		return m, tea.Quit
	case "up":
		m.handle_up()
	case "down":
		m.handle_down()
	case "right":
		m.handle_right()
	case "left":
		m.handle_left()
	}
	return m, nil
}
func (m *TimeSurvey) genarateCursorIfPointed(component string) string {
	if m.cursor == component {
		return ">"
	}
	return " "
}
func (m *TimeSurvey) View() string {
	var s string
	s += m.genarateCursorIfPointed(day) + fmt.Sprintf("%d", m.CurrentTime.Day())
	s += "/"
	s += m.genarateCursorIfPointed(month) + fmt.Sprintf("%d", m.CurrentTime.Month())
	s += "/"
	s += m.genarateCursorIfPointed(year) + fmt.Sprintf("%d", m.CurrentTime.Year())
	s += m.genarateCursorIfPointed(hour) + fmt.Sprintf("%d", m.CurrentTime.Hour())
	s += ":"
	s += m.genarateCursorIfPointed(minute) + fmt.Sprintf("%d", m.CurrentTime.Minute())
	return s
}
