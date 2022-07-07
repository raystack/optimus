package playground

import (
	"time"
    "strconv"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/odpf/optimus/cmd/survey"
)

type model struct {
	dStart        time.Time
	dEnd          time.Time
	windowSize    int
	offset        int
	truncate       string
	sechduledDate time.Time
	increaseBy    int
	cursor        string
	window        Window
}

func initialModel() model {
	surveyForInitilization := survey.WindowSurvey{}
	window := Window{surveyForInitilization}
	return model{
		dStart:        time.Now(),
		dEnd:          time.Now(),
		windowSize:    0,
		offset:        0,
		truncate:       "hour",
		sechduledDate: time.Now(),
		increaseBy:    1,
		cursor:        "windowSize",
        window:        window,
	}
}
func (m model) Init() tea.Cmd {
	// Just return `nil`, which means "no I/O right now, please."
	return nil
}
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	// Is it a key press?
	case tea.KeyMsg:

		// Cool, what was the actual key pressed?
		switch msg.String() {
		case "ctrl+c", "q":
			return m, tea.Quit
		case "up":
			switch m.cursor {
			case "windowSize":
				m.cursor = "increaseBy"
			case "offset":
				m.cursor = "windowSize"
			case "truncate":
				m.cursor = "offset"
			case "sechduledDate":
				m.cursor = "truncate"
			case "increaseBy":
				m.cursor = "sechduledDate"
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
				m.cursor = "windowSize"
			}
		case "left":
			switch m.cursor {
			case "windowSize":
				if (m.windowSize >= m.increaseBy){
				m.windowSize -= m.increaseBy
				}
			case "offset":
				m.offset -= m.increaseBy
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
				m.sechduledDate = 	m.sechduledDate.Add(time.Duration(m.increaseBy) * time.Hour)
			case "increaseBy":
				if (m.increaseBy > 1){
				m.increaseBy--
				}
			}
			m.dEnd     = m.window.truncate(m.sechduledDate,m.truncate)
			m.dEnd     = m.window.applyoffset(m.dEnd,m.offset)
			m.dStart   = m.dEnd.Add(time.Duration(-1*m.windowSize) * time.Hour) 
		case "right":
			switch m.cursor {
			case "windowSize":
				m.windowSize += m.increaseBy
			case "offset":
				m.offset += m.increaseBy
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
			m.sechduledDate = 	m.sechduledDate.Add(time.Duration(-1*m.increaseBy) * time.Hour)
			case "increaseBy":
				m.increaseBy++
			}
			m.dEnd     = m.window.truncate(m.sechduledDate,m.truncate)
			m.dEnd     = m.window.applyoffset(m.dEnd,m.offset)
			m.dStart   = m.dEnd.Add(time.Duration(-1*m.windowSize) * time.Hour)
		}
	}
	return m, nil
}
func (m model) View() string {
	s := ""
    s = "dStart : " + m.dStart.Format("01-02-2006 15:04")+"     "+ "dEnd : " +m.dEnd.Format("01-02-2006 15:04");
	s += "\n"
	if m.cursor == "windowSize"{
		s += ">"
	} else{
		s += " "
	}
	s += "windowSize     "
	s += strconv.Itoa(m.windowSize)
	s += "\n"
	if m.cursor == "offset"{
		s += ">"
	} else{
		s += " "
	}
	s += "offset         "
	s += strconv.Itoa(m.offset)
    s += "\n"
	if m.cursor == "truncate"{
		s += ">"
	} else{
		s += " "
	}
	s += "truncate       "
	s += m.truncate
	s += "\n"
	if m.cursor == "sechduledDate"{
		s += ">"
	} else{
		s += " "
	}
	s += "sechduled date "
	s += m.sechduledDate.Format("01-02-2006 15:04")
	s += "\n"
	if m.cursor == "increaseBy"{
		s += ">"
	} else{
		s += " "
	}
	s += "increase by    "
	s += strconv.Itoa(m.increaseBy)
	s += "\n"
	return s
}

