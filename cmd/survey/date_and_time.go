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

func GetTimeSurvey() *TimeSurvey {
	return &TimeSurvey{time.Now(), day}
}

type TimeSurvey struct {
	CurrentTime time.Time
	cursor      string
}

func (TimeSurvey) Init() tea.Cmd {
	return nil
}
func (m *TimeSurvey) handleUp() {
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
func (m *TimeSurvey) handleDown() {
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
func (m *TimeSurvey) handleLeft() {
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
func (m *TimeSurvey) handleRight() {
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
	case "ctrl+c", "enter":
		return m, tea.Quit
	case "up":
		m.handleUp()
	case "down":
		m.handleDown()
	case "right":
		m.handleRight()
	case "left":
		m.handleLeft()
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
