package playground

import (
	"strconv"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/odpf/optimus/job"
)

type model struct {
	cursor string
	state  state
}

func initialModel() model {
	windowv1 := job.WindowV1{Size: "0M0h", Offset: "0M0h", TruncateTo: ""}
	windowv2 := job.WindowV2{Size: "0M0h", Offset: "0M0h", TruncateTo: ""}
	return model{
		cursor: "window-hour",
		state:  state{windowv1: windowv1, windowv2: windowv2, sechduledDate: time.Now()},
	}
}

func (m model) Init() tea.Cmd {
	return nil
}
func (m model) handleUp(cursor string) string {
	switch cursor {
	case "year", "month", "day", "hour", "minute":
		return "truncate"
	case "truncate":
		return "offset-month"
	case "offset-month":
		return "window-month"
	case "offset-hour":
		return "window-hour"
	}
	return cursor
}
func (m model) handleDown(cursor string) string {
	switch cursor {
	case "truncate":
		return "hour"
	case "offset-month":
		return "truncate"
	case "offset-hour":
		return "truncate"
	case "window-month":
		return "offset-month"
	case "window-hour":
		return "offset-hour"
	}
	return cursor
}
func (m model) handleRight(cursor string) string {
	switch cursor {
	case "offset-month":
		return "offset-hour"
	case "window-month":
		return "window-hour"
	case "month":
		return "year"
	case "day":
		return "month"
	case "minute":
		return "day"
	case "hour":
		return "minute"
	}
	return cursor
}
func (m model) handleLeft(cursor string) string {
	switch cursor {
	case "offset-hour":
		return "offset-month"
	case "window-hour":
		return "window-month"
	case "year":
		return "month"
	case "month":
		return "day"
	case "day":
		return "minute"
	case "minute":
		return "hour"
	}
	return cursor
}
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "up":
			m.cursor = m.handleUp(m.cursor)
		case "down":
			m.cursor = m.handleDown(m.cursor)
		case "left":
			m.cursor = m.handleLeft(m.cursor)
		case "right":
			m.cursor = m.handleRight(m.cursor)
		case "shift+up", "shift+right":
			switch m.cursor {
			case "window-hour":
				m.state.windowv2.Size = m.state.IncrementHour(m.state.windowv2.Size)
			case "window-month":
				m.state.windowv2.Size = m.state.IncrementMonth(m.state.windowv2.Size)
			case "offset-hour":
				m.state.windowv2.Offset = m.state.IncrementHour(m.state.windowv2.Offset)
			case "offset-month":
				m.state.windowv2.Offset = m.state.IncrementMonth(m.state.windowv2.Offset)
			case "truncate":
				m.state.windowv2.TruncateTo = m.state.IncrementTruncate()
			case "year", "month", "day", "hour", "minute":
				m.state.sechduledDate = m.state.IncrementDate(m.cursor)
			}
		case "shift+down", "shift+left":
			switch m.cursor {
			case "window-hour":
				m.state.windowv2.Size = m.state.DecrementHour(m.state.windowv2.Size)
			case "window-month":
				m.state.windowv2.Size = m.state.DecrementMonth(m.state.windowv2.Size)
			case "offset-hour":
				m.state.windowv2.Offset = m.state.DecrementHour(m.state.windowv2.Offset)
			case "offset-month":
				m.state.windowv2.Offset = m.state.DecrementMonth(m.state.windowv2.Offset)
			case "truncate":
				m.state.windowv2.TruncateTo = m.state.DecrementTruncate()
			case "year", "month", "day", "hour", "minute":
				m.state.sechduledDate = m.state.DecrementDate(m.cursor)
			}
		}
	}
	return m, nil
}
func (m model) genarateCursor(current string) string {
	if m.cursor == current {
		return ">"
	}
	return " "
}
func (m model) genarateSechduledDateView() string {
	s := ""
	s += m.genarateCursor("hour")
	s += strconv.Itoa(m.state.sechduledDate.Hour())
	s += m.genarateCursor("minute")
	s += strconv.Itoa(m.state.sechduledDate.Minute())
	s += m.genarateCursor("day")
	s += strconv.Itoa(m.state.sechduledDate.Day())
	s += m.genarateCursor("month")
	s += m.state.sechduledDate.Month().String()
	s += m.genarateCursor("year")
	s += strconv.Itoa(m.state.sechduledDate.Year())
	date_format := "DATE FORMAT :  HH mm DD MM YYYY"
	s += " " + date_format
	s += "\n"
	return s
}
func (m model) View() string {
	s := ""
	s += "Size               "
	months, hours := m.state.getMonthsAndHours(m.state.windowv2.Size)
	s += m.genarateCursor("window-month")
	s += months + "M" + " "
	s += m.genarateCursor("window-hour")
	s += hours + "h"
	s += "\n"
	s += "Offset             "
	months, hours = m.state.getMonthsAndHours(m.state.windowv2.Offset)
	s += m.genarateCursor("offset-month")
	s += months + "M" + " "
	s += m.genarateCursor("offset-hour")
	s += hours + "h"
	s += "\n"
	s += "TruncateTo         "
	s += m.genarateCursor("truncate")
	s += m.state.windowv2.TruncateTo
	s += "\n"
	s += "sechduled-date     "
	s += m.genarateSechduledDateView()
	s += "\n\n"

	s += "     " + m.state.genarateV1TimeRange()
	s += "     " + m.state.genarateV2TimeRange()
	return s
}
