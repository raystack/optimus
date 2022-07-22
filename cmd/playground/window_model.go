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

type cursorType struct {
	PointedAt cursorPointer
}

// model contains a cursor to keep track of the pointer, two input fields for taking Size and Offset as input
// model also contains a state which maintains both versions of windows and the sechdule time
type model struct {
	cursor      cursorType
	state       state
	sizeInput   textinput.Model
	offsetInput textinput.Model
}

func initModel() *model {
	windowV1 := job.WindowV1{Size: defaultSize.getStringValue(), Offset: defaultSize.getStringValue(), TruncateTo: defaultTruncate.getStringValue()}
	windowV2 := job.WindowV2{Size: defaultSize.getStringValue(), Offset: defaultSize.getStringValue(), TruncateTo: defaultTruncate.getStringValue()}
	sizeInput := textinput.New()
	sizeInput.Placeholder = defaultSize.getStringValue()
	sizeInput.Focus()
	offsetInput := textinput.New()
	offsetInput.Placeholder = defaultSize.getStringValue()
	return &model{
		cursor:      cursorType{size},
		state:       state{windowV1: windowV1, windowV2: windowV2, scheduledTime: time.Now()},
		sizeInput:   sizeInput,
		offsetInput: offsetInput,
	}
}

// since we don not intend to keep any input after the process is killed we return nil
func (*model) Init() tea.Cmd {
	return nil
}

// this handles motion of the pointer when we hit arrow-up key
func (m *model) handleUp() {
	switch m.cursor.PointedAt {
	case offset:
		// here we are shifting from Offsetinput to Sizeinput so we have to stop updating the Offsetinput(hence Offset.Blur())
		// and start updating Sizeinput(hence Sizeinput.Focus())
		m.offsetInput.Blur()
		m.sizeInput.Focus()
		m.cursor.PointedAt = size
	case truncateTo:
		m.offsetInput.Focus()
		m.cursor.PointedAt = offset
	case year, month, day, hour, minute:
		m.cursor.PointedAt = truncateTo
	}
}

func (m *model) handleDown() {
	switch m.cursor.PointedAt {
	case truncateTo:
		m.cursor.PointedAt = hour
	case offset:
		m.offsetInput.Blur()
		m.cursor.PointedAt = truncateTo
	case size:
		m.offsetInput.Focus()
		m.sizeInput.Blur()
		m.cursor.PointedAt = offset
	}
}

// handles left and right arrow key movements
func (m *model) handleRight() {
	switch m.cursor.PointedAt {
	case month:
		m.cursor.PointedAt = year
	case day:
		m.cursor.PointedAt = month
	case minute:
		m.cursor.PointedAt = day
	case hour:
		m.cursor.PointedAt = minute
	}
}

func (m *model) handleLeft() {
	switch m.cursor.PointedAt {
	case minute:
		m.cursor.PointedAt = hour
	case day:
		m.cursor.PointedAt = minute
	case month:
		m.cursor.PointedAt = day
	case year:
		m.cursor.PointedAt = month
	}
}
func (m *model) handleIncrease() {
	switch m.cursor.PointedAt {
	case truncateTo:
		m.state.IncrementTruncate()
	case year, month, day, hour, minute:
		m.state.IncrementTime(m.cursor.PointedAt.getStringValue())
	}
}
func (m *model) handleDecrease() {
	switch m.cursor.PointedAt {
	case truncateTo:
		m.state.DecrementTruncate()
	case year, month, day, hour, minute:
		m.state.DecrementTime(m.cursor.PointedAt.getStringValue())
	}
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	// switch currMsg := msg.(type) {
	// case tea.KeyMsg:
	currMsg := reflect.TypeOf(msg)
	if currMsg.String() == "tea.KeyMsg" {
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
		case "M", "h", "-", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0", "backspace":
			// to get updated the input fields must be focussed since we handle the Focus and Blur we can directly update them here
			m.sizeInput, _ = m.sizeInput.Update(msg)
			m.offsetInput, _ = m.offsetInput.Update(msg)
			m.state.updateWindowparameters(m.sizeInput.Value(), m.offsetInput.Value())
		}
	}
	return m, nil
}

func (m *model) genarateCursorIfPointed(position string, value string) string {
	if m.cursor.PointedAt.getStringValue() == position {
		var s strings.Builder
		s.WriteString("[")
		s.WriteString(value)
		s.WriteString("]")
		return s.String()
	}
	return " " + value
}

// we genare a string representing the sechduled time , which also adds a cursor if it is pointing to the any of the fields in sechdueld date
func (m *model) genarateSechduledDateView() string {
	var s strings.Builder
	s.WriteString(m.genarateCursorIfPointed(hour.getStringValue(), strconv.Itoa(m.state.scheduledTime.Hour())))
	s.WriteString(":")
	s.WriteString(m.genarateCursorIfPointed(minute.getStringValue(), strconv.Itoa(m.state.scheduledTime.Minute())))
	s.WriteString(m.genarateCursorIfPointed(day.getStringValue(), strconv.Itoa(m.state.scheduledTime.Day())))
	s.WriteString(m.genarateCursorIfPointed(month.getStringValue(), m.state.scheduledTime.Month().String()))
	s.WriteString(m.genarateCursorIfPointed(year.getStringValue(), strconv.Itoa(m.state.scheduledTime.Year())))
	return s.String()
}

// this will update the values of Size and offset of the window versions present in state to the new values taken from the input

// this will  be invoked for every update
func (m *model) View() string {
	var s strings.Builder
	s.WriteString("Size\t\t")
	s.WriteString(m.sizeInput.View())
	s.WriteString("\n")
	s.WriteString("Offset\t\t")
	s.WriteString(m.offsetInput.View())
	s.WriteString("\n")
	s.WriteString("TruncateTo\t ")
	s.WriteString(m.genarateCursorIfPointed(truncateTo.getStringValue(), m.state.windowV2.TruncateTo))
	s.WriteString("\n")
	s.WriteString("sechduled date   ")
	s.WriteString(m.genarateSechduledDateView())
	s.WriteString("\n\n\n             ")
	// calculate the value of dstart and dend from the imported window versions
	dStartV1, dEndV1, err := m.state.windowV1.GetTimeRange(m.state.scheduledTime)
	if err != nil {
		// if the window v1 cannot get time range from the given input it displays this error
		s.WriteString("current window version does not support the above input")
	} else {
		s.WriteString("dStartV1 :" + dStartV1.Format("15:04 2006-01-02") + "     dEndV1 :" + dEndV1.Format("15:04 2006-01-02"))
	}
	s.WriteString("\n             ") // extra space added for design reasons
	dStartV2, dEndV2, err := m.state.windowV2.GetTimeRange(m.state.scheduledTime)
	if err != nil {
		s.WriteString(err.Error())
	} else {
		s.WriteString("dStartV2 :" + dStartV2.Format("15:04 2006-01-02") + "     dEndV2 :" + dEndV2.Format("15:04 2006-01-02"))
	}
	s.WriteString("\n")
	return s.String()
}
