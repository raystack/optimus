package playground

import (
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/odpf/optimus/job"
)

// model contains a cursor to keep track of the pointer, two input fields for taking Size and Offset as input
// model also contains a state which maintains both versions of windows and the sechdule time
type model struct {
	cursor      string
	state       state
	Sizeinput   textinput.Model
	Offsetinput textinput.Model
}

func initialModel() model {
	windowv1 := job.WindowV1{Size: "0M0h", Offset: "0M0h", TruncateTo: "h"}
	windowv2 := job.WindowV2{Size: "0M0h", Offset: "0M0h", TruncateTo: "h"}
	Sizeinput := textinput.New()
	Sizeinput.CharLimit = 32
	Sizeinput.Placeholder = "0M0h"
	Sizeinput.Focus()
	Offsetinput := textinput.New()
	Offsetinput.CharLimit = 32
	Offsetinput.Placeholder = "0M0h"
	return model{
		cursor:      "Sizeinput",
		state:       state{windowv1: windowv1, windowv2: windowv2, sechduledTime: time.Now()},
		Sizeinput:   Sizeinput,
		Offsetinput: Offsetinput,
	}
}

// since we don not intend to keep any input after the process is killed we return nil
func (m model) Init() tea.Cmd {
	return nil
}

// this handles motion of the pointer when we hit arrow-up key
func (m *model) handleUp() {
	switch m.cursor {
	case "Offsetinput":
		//here we are shifting from Offsetinput to Sizeinput so we have to stop updating the Offsetinput(hence Offset.Blur())
		// and start updating Sizeinput(hence Sizeinput.Focus())
		m.Offsetinput.Blur()
		m.Sizeinput.Focus()
		m.cursor = "Sizeinput"
	case "TruncateTo":
		m.Offsetinput.Focus()
		m.cursor = "Offsetinput"
	case "year", "month", "day", "hour", "minute":
		m.cursor = "TruncateTo"
	}
}

func (m *model) handleDown() {
	switch m.cursor {
	case "TruncateTo":
		m.cursor = "hour"
	case "Offsetinput":
		m.Offsetinput.Blur()
		m.cursor = "TruncateTo"
	case "Sizeinput":
		m.Offsetinput.Focus()
		m.Sizeinput.Blur()
		m.cursor = "Offsetinput"
	}
}

// handles left and right arrow key movements
func (m *model) handleRight() {
	switch m.cursor {
	case "month":
		m.cursor = "year"
	case "day":
		m.cursor = "month"
	case "minute":
		m.cursor = "day"
	case "hour":
		m.cursor = "minute"
	}
}

func (m *model) handleLeft() {
	switch m.cursor {
	case "minute":
		m.cursor = "hour"
	case "day":
		m.cursor = "minute"
	case "month":
		m.cursor = "day"
	case "year":
		m.cursor = "month"
	}
}

// update the state of the system when a msg(Key strokes , mouse clicks ..) is given
func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	// respond to only key strokes
	case tea.KeyMsg:
		switch msg.String() {
		// to terminate the process
		case "ctrl+c", "q":
			return m, tea.Quit
		case "up":
			m.handleUp()
		case "down":
			m.handleDown()
		case "left":
			m.handleLeft()
		case "right":
			m.handleRight()
		// increase or decrease the value of the parameter the cursor points at
		case "shift+up", "shift+right":
			switch m.cursor {
			case "TruncateTo":
				m.state.windowv2.TruncateTo = m.state.IncrementTruncate()
			case "year", "month", "day", "hour", "minute":
				m.state.IncrementTime(m.cursor)
			}
		case "shift+down", "shift+left":
			switch m.cursor {
			case "TruncateTo":
				m.state.windowv2.TruncateTo = m.state.DecrementTruncate()
			case "year", "month", "day", "hour", "minute":
				m.state.DecrementTime(m.cursor)
			}
		// if not the arrow keys then the input must be given to the size or the off set hence we update them accordingly
		default:
			// to get updated the input fields must be focussed since we handle the Focus and Blur we can directly update them here
			m.Sizeinput, _ = m.Sizeinput.Update(msg)
			m.Offsetinput, _ = m.Offsetinput.Update(msg)
		}
	}
	// update the values of the window versions in state for every modification from input
	m.updateWindowVersions()
	return m, nil
}

// we genarate a cursor if the value of the cursor matches with the current field name, else we pass a blank instead
func (m model) genarateFields(position string, value string) string {
	if m.cursor == position {
		var s strings.Builder
		s.WriteString("[")
		s.WriteString(value)
		s.WriteString("]")
		return s.String()
	}
	return " " + value
}

// we genare a string representing the sechduled time , which also adds a cursor if it is pointing to the any of the fields in sechdueld date
func (m model) genarateSechduledDateView() string {
	var s strings.Builder
	s.WriteString(m.genarateFields("hour", strconv.Itoa(m.state.sechduledTime.Hour())))
	s.WriteString(":")
	s.WriteString(m.genarateFields("minute", strconv.Itoa(m.state.sechduledTime.Minute())))
	s.WriteString(m.genarateFields("day", strconv.Itoa(m.state.sechduledTime.Day())))
	s.WriteString(m.genarateFields("month", m.state.sechduledTime.Month().String()))
	s.WriteString(m.genarateFields("year", strconv.Itoa(m.state.sechduledTime.Year())))
	return s.String()
}

// this will update the values of Size and offset of the window versions present in state to the new values taken from the input
func (m *model) updateWindowVersions() {
	// .Value() here represents the string that is present inside the text field
	m.state.windowv1.Size = m.Sizeinput.Value()
	m.state.windowv2.Size = m.Sizeinput.Value()
	m.state.windowv1.Offset = m.Offsetinput.Value()
	m.state.windowv2.Offset = m.Offsetinput.Value()
	m.state.windowv1.TruncateTo = m.state.windowv2.TruncateTo
}

// this will  be invoked for every update
func (m model) View() string {
	var s strings.Builder
	s.WriteString("Size            ")
	s.WriteString(m.Sizeinput.View())
	s.WriteString("\n")
	s.WriteString("Offset          ")
	s.WriteString(m.Offsetinput.View())
	s.WriteString("\n")
	s.WriteString("TruncateTo       ")
	s.WriteString(m.genarateFields("TruncateTo", m.state.windowv2.TruncateTo))
	s.WriteString("\n")
	s.WriteString("sechduled date   ")
	s.WriteString(m.genarateSechduledDateView())
	s.WriteString("\n\n\n             ") // extra space added for design reasons
	// calculate the value of dstart and dend from the imported window versions
	dstartv1, dendv1, err := m.state.windowv1.GetTimeRange(m.state.sechduledTime)
	if err != nil {
		// if the window v1 cannot get time range from the given input it displays this error
		s.WriteString("current window version does not support the above input")
	} else {
		s.WriteString("dstartv1 :" + dstartv1.Format("15:04 2006-01-02") + "     dendv1 :" + dendv1.Format("15:04 2006-01-02"))
	}
	s.WriteString("\n             ") // extra space added for design reasons
	dstartv2, dendv2, err := m.state.windowv2.GetTimeRange(m.state.sechduledTime)
	if err != nil {
		s.WriteString(err.Error())
	} else {
		s.WriteString("dstartv2 :" + dstartv2.Format("15:04 2006-01-02") + "     dendv2 :" + dendv2.Format("15:04 2006-01-02"))
	}
	s.WriteString("\n")
	return s.String()
}
