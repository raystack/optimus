package playground

import (
	"strconv"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/odpf/optimus/cmd/survey"
)

type MonthHour struct {
	month int
	hour  int
}
type model struct {
	dStart         time.Time
	dEnd           time.Time
	windowSize     MonthHour
	offset         MonthHour
	truncate       string
	sechduledDate  time.Time
	increaseBy     string
	increaseDateBy string
	cursor         string
	window         Window
}

// model contains a window for using the function defined for the window struct
func initialModel() model {
	surveyForInitilization := survey.WindowSurvey{}
	window := Window{surveyForInitilization}
	return model{
		dStart:         time.Now(),
		dEnd:           time.Now(),
		windowSize:     MonthHour{0, 0},
		offset:         MonthHour{0, 0},
		truncate:       "hour",
		sechduledDate:  time.Now(),
		increaseBy:     "hour",
		increaseDateBy: "hour",
		cursor:         "windowSize",
		window:         window,
	}
}

// no input is required since we operate using key strokes
func (m model) Init() tea.Cmd {
	// Just return `nil`, which means "no I/O right now, please."
	return nil
}

/*
key controls
up, down move the cursor
left decrease
right increase
*/
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	// Is it a key press?
	case tea.KeyMsg:

		// what was the actual key pressed?
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "up":
			switch m.cursor {
			case "windowSize":
				m.cursor = "increaseDateBy"
			case "offset":
				m.cursor = "windowSize"
			case "truncate":
				m.cursor = "offset"
			case "sechduledDate":
				m.cursor = "truncate"
			case "increaseBy":
				m.cursor = "sechduledDate"
			case "increaseDateBy":
				m.cursor = "increaseBy"
			}
		case "down":
			switch m.cursor {
			case "windowSize":
				m.cursor = "offset"
			case "offset":
				m.cursor = "truncate"
			case "truncate":
				m.cursor = "sechduledDate"
			case "sechduledDate":
				m.cursor = "increaseBy"
			case "increaseBy":
				m.cursor = "increaseDateBy"
			case "increseDateBy":
				m.cursor = "increaseDateBy"
			}
		case "left":
			switch m.cursor {
			case "windowSize":
				switch m.increaseBy {
				case "hour":
					if m.windowSize.hour > 0 {
						m.windowSize.hour--
					}
				case "month":
					if m.windowSize.month > 0 {
						m.windowSize.month--
					}
				}
			case "offset":
				switch m.increaseBy {
				case "hour":
					m.offset.hour--
				case "month":
					m.offset.month--
				}
			case "truncate":
				temporary_truncate := m.truncate
				switch temporary_truncate {
				case "day":
					m.truncate = "hour"
				case "week":
					m.truncate = "day"
				case "month":
					m.truncate = "week"
				}
			case "sechduledDate":
				switch m.increaseDateBy {
				case "hour":
					m.sechduledDate = m.sechduledDate.Add(-1 * time.Hour)
				case "day":
					m.sechduledDate = m.sechduledDate.AddDate(0, 0, -1)
				case "month":
					m.sechduledDate = m.sechduledDate.AddDate(0, -1, 0)
				}
			case "increaseBy":
				switch m.increaseBy {
				case "month":
					m.increaseBy = "hour"
				}
			case "increaseDateBy":
				switch m.increaseDateBy {
				case "day":
					m.increaseDateBy = "hour"
				case "month":
					m.increaseDateBy = "day"
				}
			}
			m.dEnd = m.window.truncate(m.sechduledDate, m.truncate)
			m.dEnd = m.window.applyoffset(m.dEnd, m.offset)
			m.dStart = m.dEnd.Add(time.Duration(-1*m.windowSize.hour)*time.Hour).AddDate(0, m.windowSize.month, 0)
		case "right":
			switch m.cursor {
			case "windowSize":
				switch m.increaseBy {
				case "hour":
					m.windowSize.hour++
				case "month":
					m.windowSize.month++
				}
			case "offset":
				switch m.increaseBy {
				case "hour":
					m.offset.hour++
				case "month":
					m.offset.month++
				}
			case "truncate":
				temporary_truncate := m.truncate
				switch temporary_truncate {
				case "hour":
					m.truncate = "day"
				case "day":
					m.truncate = "week"
				case "week":
					m.truncate = "month"
				}
			case "sechduledDate":
				switch m.increaseDateBy {
				case "hour":
					m.sechduledDate = m.sechduledDate.Add(time.Hour)
				case "day":
					m.sechduledDate = m.sechduledDate.AddDate(0, 0, 1)
				case "month":
					m.sechduledDate = m.sechduledDate.AddDate(0, 1, 0)
				}
			case "increaseBy":
				switch m.increaseBy {
				case "hour":
					m.increaseBy = "month"
				}
			case "increaseDateBy":
				switch m.increaseDateBy {
				case "day":
					m.increaseDateBy = "month"
				case "hour":
					m.increaseDateBy = "day"
				}
			}
			m.dEnd = m.window.truncate(m.sechduledDate, m.truncate)
			m.dEnd = m.window.applyoffset(m.dEnd, m.offset)
			m.dStart = m.dEnd.Add(time.Duration(-1*m.windowSize.hour)*time.Hour).AddDate(0, m.windowSize.month, 0)
		}
	}
	return m, nil
}

// view takes the model and genarates a new string for every key stroke
// this gives us the illusion of some elements being static while others are moving
// but they are all being replaced continuosly in run time
func (m model) View() string {
	s := ""
	s = "dStart : " + m.dStart.Format("01-02-2006 15:04") + "     " + "dEnd : " + m.dEnd.Format("01-02-2006 15:04")
	s += "\n"
	// the below part of repitatice code is to ensure that if the cursor is currently present here we print it
	if m.cursor == "windowSize" {
		s += ">"
	} else {
		s += " "
	}
	// this acts as a menu list
	s += "windowSize          "
	s += strconv.Itoa(m.windowSize.month) + "M    " + strconv.Itoa(m.windowSize.hour) + "H"
	if m.windowSize.hour == 0 && m.windowSize.month == 0 {
		s += "        warning windowSize is 0"
	}
	s += "\n"
	if m.cursor == "offset" {
		s += ">"
	} else {
		s += " "
	}
	s += "offset              "
	s += strconv.Itoa(m.offset.month) + "M    " + strconv.Itoa(m.offset.hour) + "H"
	s += "\n"
	if m.cursor == "truncate" {
		s += ">"
	} else {
		s += " "
	}
	s += "truncate            "
	if m.truncate == "hour" {
		s += "h"
	}
	if m.truncate == "day" {
		s += "d"
	}
	if m.truncate == "week" {
		s += "w"
	}
	if m.truncate == "month" {
		s += "M"
	}
	s += "("
	s += m.truncate
	s += ")"
	s += "\n"
	if m.cursor == "sechduledDate" {
		s += ">"
	} else {
		s += " "
	}
	s += "sechduled date      "
	s += m.sechduledDate.Format("01-02-2006 15:04")
	s += "\n"
	if m.cursor == "increaseBy" {
		s += ">"
	} else {
		s += " "
	}
	s += "increase by         "
	s += m.increaseBy
	s += "\n"
	if m.cursor == "increaseDateBy" {
		s += ">"
	} else {
		s += " "
	}
	s += "increase Date by    "
	s += m.increaseDateBy
	s += "\n"
	// returnning the genarated string
	return s
}
