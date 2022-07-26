package playground

import (
	"fmt"
	"reflect"
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
	cursor      cursorPointer
	state       state
	sizeInput   textinput.Model
	offsetInput textinput.Model
}

func initModel() *model {
	windowV1 := job.WindowV1{Size: defaultSize, Offset: defaultSize, TruncateTo: defaultTruncate}
	windowV2 := job.WindowV2{Size: defaultSize, Offset: defaultSize, TruncateTo: defaultTruncate}
	sizeInput := textinput.New()
	sizeInput.Placeholder = defaultSize
	sizeInput.Focus()
	offsetInput := textinput.New()
	offsetInput.Placeholder = defaultSize
	return &model{
		cursor:      pointToSizeInput,
		state:       state{windowV1: windowV1, windowV2: windowV2, scheduledTime: time.Now()},
		sizeInput:   sizeInput,
		offsetInput: offsetInput,
	}
}

func (*model) Init() tea.Cmd {
	return nil
}

func (m *model) handleUp() {
	switch m.cursor {
	case pointToOffsetInput:
		// here we are shifting from Offsetinput to Sizeinput so we have to stop updating the Offsetinput(hence Offset.Blur())
		// and start updating Sizeinput(hence Sizeinput.Focus())
		m.offsetInput.Blur()
		m.sizeInput.Focus()
		m.cursor = pointToSizeInput
	case pointToTruncate:
		m.offsetInput.Focus()
		m.cursor = pointToOffsetInput
	case pointToYear, pointToMonth, PointToDay, pointToHour, pointToMinute:
		m.cursor = pointToTruncate
	}
}

func (m *model) handleDown() {
	switch m.cursor {
	case pointToTruncate:
		m.cursor = pointToHour
	case pointToOffsetInput:
		m.offsetInput.Blur()
		m.cursor = pointToTruncate
	case pointToSizeInput:
		m.offsetInput.Focus()
		m.sizeInput.Blur()
		m.cursor = pointToOffsetInput
	}
}

func (m *model) handleRight() {
	switch m.cursor {
	case pointToMonth:
		m.cursor = pointToYear
	case PointToDay:
		m.cursor = pointToMonth
	case pointToMinute:
		m.cursor = PointToDay
	case pointToHour:
		m.cursor = pointToMinute
	}
}

func (m *model) handleLeft() {
	switch m.cursor {
	case pointToMinute:
		m.cursor = pointToHour
	case PointToDay:
		m.cursor = pointToMinute
	case pointToMonth:
		m.cursor = PointToDay
	case pointToYear:
		m.cursor = pointToMonth
	}
}
func (m *model) handleIncrease() {
	switch m.cursor {
	case pointToTruncate:
		m.state.incrementTruncate()
	case pointToYear, pointToMonth, PointToDay, pointToHour, pointToMinute:
		m.state.incrementScheduleTimeOn(m.cursor)
	}
}
func (m *model) handleDecrease() {
	switch m.cursor {
	case pointToTruncate:
		m.state.decrementTruncate()
	case pointToYear, pointToMonth, PointToDay, pointToHour, pointToMinute:
		m.state.decrementScheduleTimeOn(m.cursor)
	}
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	currMsg := reflect.TypeOf(msg)
	if currMsg.String() != "tea.KeyMsg" {
		return m, nil
	}
	switch fmt.Sprintf("%s", msg) {
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
	case "shift+up", "shift+right":
		m.handleIncrease()
	case "shift+down", "shift+left":
		m.handleDecrease()
	case "M", "h", "-",
		"1", "2", "3", "4", "5",
		"6", "7", "8", "9", "0",
		"backspace":
		// to get updated the input fields must be focussed since we handle the Focus and Blur we can directly update them here
		m.sizeInput, _ = m.sizeInput.Update(msg)
		m.offsetInput, _ = m.offsetInput.Update(msg)
		m.state.updateWindowparameters(m.sizeInput.Value(), m.offsetInput.Value())
	}
	return m, nil
}

func (m *model) genarateCursorIfPointed(position string, value string) string {
	if m.cursor.getStringValue() == position {
		var s strings.Builder
		s.WriteString("[")
		s.WriteString(value)
		s.WriteString("]")
		return s.String()
	}
	return " " + value
}

// generateSechduledDateView generates a string representing the sechduled time , which also adds a cursor if it is pointing to the any of the fields in sechdueld date
func (m *model) generateSechduledDateView() string {
	var s strings.Builder
	s.WriteString(m.genarateCursorIfPointed(pointToHour.getStringValue(), strconv.Itoa(m.state.scheduledTime.Hour())))
	s.WriteString(":")
	s.WriteString(m.genarateCursorIfPointed(pointToMinute.getStringValue(), strconv.Itoa(m.state.scheduledTime.Minute())))
	s.WriteString(m.genarateCursorIfPointed(PointToDay.getStringValue(), strconv.Itoa(m.state.scheduledTime.Day())))
	s.WriteString(m.genarateCursorIfPointed(pointToMonth.getStringValue(), m.state.scheduledTime.Month().String()))
	s.WriteString(m.genarateCursorIfPointed(pointToYear.getStringValue(), strconv.Itoa(m.state.scheduledTime.Year())))
	return s.String()
}

// View() update the values of Size and offset of the window versions present in state to the new values taken from the input

func (m *model) View() string {
	var s strings.Builder
	s.WriteString("Size\t\t")
	s.WriteString(m.sizeInput.View())
	s.WriteString("\n")
	s.WriteString("Offset\t\t")
	s.WriteString(m.offsetInput.View())
	s.WriteString("\n")
	s.WriteString("TruncateTo\t ")
	s.WriteString(m.genarateCursorIfPointed(pointToTruncate.getStringValue(), m.state.windowV2.TruncateTo))
	s.WriteString("\n")
	s.WriteString("sechduled date   ")
	s.WriteString(m.generateSechduledDateView())
	s.WriteString("\n\n\n\t\t")
	// calculate the value of dstart and dend from the imported window versions
	dStartV1, dEndV1, err := m.state.windowV1.GetTimeRange(m.state.scheduledTime)
	if err != nil {
		// if the window v1 cannot get time range from the given input it displays this error
		s.WriteString("current window version does not support the above input")
	} else {
		s.WriteString("dStartV1 :" + dStartV1.Format("15:04 2006-01-02") + "     dEndV1 :" + dEndV1.Format("15:04 2006-01-02"))
	}
	s.WriteString("\n\t\t")
	dStartV2, dEndV2, err := m.state.windowV2.GetTimeRange(m.state.scheduledTime)
	if err != nil {
		s.WriteString(err.Error())
	} else {
		s.WriteString("dStartV2 :" + dStartV2.Format("15:04 2006-01-02") + "     dEndV2 :" + dEndV2.Format("15:04 2006-01-02"))
	}
	s.WriteString("\n")
	return s.String()
}
