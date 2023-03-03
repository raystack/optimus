package window

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/goto/salt/log"
	"github.com/olekukonko/tablewriter"

	"github.com/goto/optimus/internal/models"
)

type model struct {
	log           log.Logger
	currentCursor cursorPointer

	truncateTo  truncateTo
	sizeInput   textinput.Model
	offsetInput textinput.Model

	scheduledTime time.Time
}

func newModel(log log.Logger) *model {
	offsetInput := textinput.New()

	sizeInput := textinput.New()
	sizeInput.Focus()

	return &model{
		log:           log,
		currentCursor: pointToTruncateTo,
		truncateTo:    truncateToDay,
		sizeInput:     sizeInput,
		offsetInput:   offsetInput,
		scheduledTime: time.Now(),
	}
}

func (*model) Init() tea.Cmd {
	// this method is to adhere to library contract
	return nil
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	currMsg := reflect.TypeOf(msg)
	if currMsg.String() != "tea.KeyMsg" {
		return m, nil
	}
	msgStr := fmt.Sprintf("%s", msg)
	switch msgStr {
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
	case "shift+up":
		m.handleIncrement()
	case "shift+down":
		m.handleDecrement()
	case "M", "h", "-",
		"1", "2", "3", "4", "5",
		"6", "7", "8", "9", "0",
		"backspace":
		m.sizeInput, _ = m.sizeInput.Update(msg)
		m.offsetInput, _ = m.offsetInput.Update(msg)
	}
	return m, nil
}

func (m *model) View() string {
	var s strings.Builder
	s.WriteString("INPUT")
	s.WriteString("\n")
	s.WriteString(m.generateWindowInputView())
	s.WriteString("\n")
	s.WriteString(m.generateWindowInputHintView())
	s.WriteString("\n")
	s.WriteString("RESULT")
	s.WriteString("\n")
	s.WriteString(m.generateWindowResultView())
	s.WriteString("\n")
	s.WriteString("DOCUMENTATION:\n")
	s.WriteString("- https://goto.github.io/optimus/docs/concepts/intervals-and-windows")
	return s.String()
}

func (m *model) generateWindowResultView() string {
	buff := &bytes.Buffer{}
	table := tablewriter.NewWriter(buff)
	table.SetHeader([]string{"Version", "Start Time", "End Time"})
	table.SetAutoMergeCells(true)
	table.Append(m.generateWindowTableRowView(1))
	table.Append(m.generateWindowTableRowView(2)) //nolint: gomnd
	table.Render()
	return buff.String()
}

func (m *model) generateWindowInputHintView() string {
	var hint string
	switch m.currentCursor {
	case pointToTruncateTo:
		hint = `valid values are:
- M: truncate to month
- w: truncate to week
- d: truncate to day
- h: truncate to hour

press shift+up to increment value
press shift+down to decrement value
`
	case pointToOffset:
		hint = `valid formats are:
- nMmh: example 1M2h meaning 1 month 2 hours
- mh: example 2h meaning 2 hours

n can be negative, while m can be negative only if nM does NOT exist
`
	case pointToSize:
		hint = `valid formats are:
- nMmh: example 1M2h meaning 1 month 2 hours
- mh: example 2h meaning 2 hours

both n and m can NOT be negative
`
	case pointToYear:
		hint = `year of the scheduled time

press shift+up to increment value
press shift+down to decrement value
`
	case pointToMonth:
		hint = `month of the scheduled time

press shift+up to increment value
press shift+down to decrement value
`
	case pointToDay:
		hint = `day of the scheduled time

press shift+up to increment value
press shift+down to decrement value
`
	case pointToHour:
		hint = `hour of the scheduled time

press shift+up to increment value
press shift+down to decrement value
`
	case pointToMinute:
		hint = `minute of the scheduled time

press shift+up to increment value
press shift+down to decrement value
`
	}
	return hint
}

func (m *model) generateWindowInputView() string {
	buff := &bytes.Buffer{}
	table := tablewriter.NewWriter(buff)
	table.SetRowLine(true)
	table.Append([]string{
		"truncate_to",
		m.generateValueWithCursorPointerView(pointToTruncateTo, string(m.truncateTo)),
	})
	table.Append([]string{
		"offset",
		m.generateValueWithCursorPointerView(pointToOffset, m.offsetInput.Value()),
	})
	table.Append([]string{
		"size",
		m.generateValueWithCursorPointerView(pointToSize, m.sizeInput.Value()),
	})
	table.Append([]string{
		"job schedule",
		m.generateSechduledTimeView(),
	})
	table.Render()
	return buff.String()
}

func (m *model) generateWindowTableRowView(version int) []string {
	window, err := models.NewWindow(version, string(m.truncateTo), m.offsetInput.Value(), m.sizeInput.Value())
	if err != nil {
		return []string{fmt.Sprintf("%d", version), err.Error()}
	}
	var startTimeRow string
	if startTime, err := window.GetStartTime(m.scheduledTime); err != nil {
		startTimeRow = err.Error()
	} else {
		startTimeRow = startTime.Format(time.RFC3339)
	}
	var endTimeRow string
	if endTime, err := window.GetEndTime(m.scheduledTime); err != nil {
		endTimeRow = err.Error()
	} else {
		endTimeRow = endTime.Format(time.RFC3339)
	}
	return []string{fmt.Sprintf("%d", version), startTimeRow, endTimeRow}
}

func (m *model) generateSechduledTimeView() string {
	year := m.generateValueWithCursorPointerView(pointToYear, strconv.Itoa(m.scheduledTime.Year()))
	month := m.generateValueWithCursorPointerView(pointToMonth, m.scheduledTime.Month().String())
	day := m.generateValueWithCursorPointerView(pointToDay, strconv.Itoa(m.scheduledTime.Day()))
	hour := m.generateValueWithCursorPointerView(pointToHour, strconv.Itoa(m.scheduledTime.Hour()))
	minute := m.generateValueWithCursorPointerView(pointToMinute, strconv.Itoa(m.scheduledTime.Minute()))
	return year + " " + month + " " + day + " " + hour + ":" + minute
}

func (m *model) generateValueWithCursorPointerView(targetCursor cursorPointer, value string) string {
	if m.currentCursor == targetCursor {
		var s strings.Builder
		s.WriteString("[")
		s.WriteString(value)
		s.WriteString("]")
		return s.String()
	}
	return value
}

func (m *model) handleDecrement() {
	switch m.currentCursor {
	case pointToTruncateTo:
		m.decrementTruncateTo()
	default:
		m.decrementScheduledTime()
	}
}

func (m *model) decrementScheduledTime() {
	switch m.currentCursor {
	case pointToMinute:
		m.scheduledTime = m.scheduledTime.Add(-1 * time.Minute)
	case pointToHour:
		m.scheduledTime = m.scheduledTime.Add(-1 * time.Hour)
	case pointToDay:
		m.scheduledTime = m.scheduledTime.AddDate(0, 0, -1)
	case pointToMonth:
		m.scheduledTime = m.scheduledTime.AddDate(0, -1, 0)
	case pointToYear:
		m.scheduledTime = m.scheduledTime.AddDate(-1, 0, 0)
	}
}

func (m *model) decrementTruncateTo() {
	switch m.truncateTo {
	case truncateToMonth:
		m.truncateTo = truncateToWeek
	case truncateToWeek:
		m.truncateTo = truncateToDay
	case truncateToDay:
		m.truncateTo = truncateToHour
	case truncateToHour:
		m.truncateTo = truncateToMonth
	}
}

func (m *model) handleIncrement() {
	switch m.currentCursor {
	case pointToTruncateTo:
		m.incrementTruncateTo()
	default:
		m.incrementScheduledTime()
	}
}

func (m *model) incrementScheduledTime() {
	switch m.currentCursor {
	case pointToMinute:
		m.scheduledTime = m.scheduledTime.Add(time.Minute)
	case pointToHour:
		m.scheduledTime = m.scheduledTime.Add(time.Hour)
	case pointToDay:
		m.scheduledTime = m.scheduledTime.AddDate(0, 0, 1)
	case pointToMonth:
		m.scheduledTime = m.scheduledTime.AddDate(0, 1, 0)
	case pointToYear:
		m.scheduledTime = m.scheduledTime.AddDate(1, 0, 0)
	}
}

func (m *model) incrementTruncateTo() {
	switch m.truncateTo {
	case truncateToHour:
		m.truncateTo = truncateToDay
	case truncateToDay:
		m.truncateTo = truncateToWeek
	case truncateToWeek:
		m.truncateTo = truncateToMonth
	case truncateToMonth:
		m.truncateTo = truncateToHour
	}
}

func (m *model) handleRight() {
	switch m.currentCursor {
	case pointToYear:
		m.currentCursor = pointToMonth
	case pointToMonth:
		m.currentCursor = pointToDay
	case pointToDay:
		m.currentCursor = pointToHour
	case pointToHour:
		m.currentCursor = pointToMinute
	case pointToMinute:
		m.currentCursor = pointToYear
	}
}

func (m *model) handleLeft() {
	switch m.currentCursor {
	case pointToMinute:
		m.currentCursor = pointToHour
	case pointToHour:
		m.currentCursor = pointToDay
	case pointToDay:
		m.currentCursor = pointToMonth
	case pointToMonth:
		m.currentCursor = pointToYear
	case pointToYear:
		m.currentCursor = pointToMinute
	}
}

func (m *model) handleDown() {
	switch m.currentCursor {
	case pointToTruncateTo:
		m.offsetInput.Focus()
		m.currentCursor = pointToOffset
	case pointToOffset:
		m.offsetInput.Blur()
		m.sizeInput.Focus()
		m.currentCursor = pointToSize
	case pointToSize:
		m.sizeInput.Blur()
		m.currentCursor = pointToYear
	default:
		m.currentCursor = pointToTruncateTo
	}
}

func (m *model) handleUp() {
	switch m.currentCursor {
	case pointToTruncateTo:
		m.currentCursor = pointToYear
	case pointToOffset:
		m.offsetInput.Blur()
		m.currentCursor = pointToTruncateTo
	case pointToSize:
		m.sizeInput.Blur()
		m.offsetInput.Focus()
		m.currentCursor = pointToOffset
	default:
		m.sizeInput.Focus()
		m.currentCursor = pointToSize
	}
}
