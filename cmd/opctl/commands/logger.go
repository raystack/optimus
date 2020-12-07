package commands

import (
	"fmt"
	"io"

	"github.com/gernest/wow"
	"github.com/gernest/wow/spin"
)

type logger interface {
	Print(...interface{})
	Println(...interface{})
	Printf(string, ...interface{})
	Writer() io.Writer
}

type spinner interface {
	Start()
	SpinInline(...interface{}) // print and overwrite on same line [require tick to print]
	Spin(...interface{})       // print on next line with info prefix [require tick to print]
	Error(...interface{})      // print without spinner with error prefix
	Log(...interface{})        // print without spinner with no prefix
	LogRaw(...interface{})     // print without spinner without dash
	StopWithSuccess(string)    // stop with success prefix
	StopWithFailure(string)    // stop with failure prefix
	Stop()                     // stop spinner
}

// keep text line short when spinner is used, can cause an issue of printing
// ever frame of spinner on new line
type wowSpinner struct {
	w             *wow.Wow
	out           io.Writer
	spinning      bool
	InfoPrefix    string
	ErrorPrefix   string
	SuccessPrefix string
	FailedPrefix  string
}

func (s *wowSpinner) Start() {
	loader := spin.Get(spin.Line)
	loader.Interval = 100
	s.w = wow.New(s.out, loader, "")
}

func (s *wowSpinner) SpinInline(args ...interface{}) {
	s.w.Start()
	s.spinning = true
	s.write(fmt.Sprint(args...), "")
}

func (s *wowSpinner) Spin(args ...interface{}) {
	if s.spinning {
		s.w.Persist()
	}
	s.w.Start()
	s.spinning = true

	s.write(fmt.Sprint(args...), s.InfoPrefix)
}

func (s *wowSpinner) Log(args ...interface{}) {
	if s.spinning {
		s.w.Persist()
		s.spinning = false
	}

	//extra space to align spinner
	fmt.Fprint(s.out, fmt.Sprintf("- %s\n", fmt.Sprint(args...)))
}

func (s *wowSpinner) LogRaw(args ...interface{}) {
	if s.spinning {
		s.w.Persist()
		s.spinning = false
	}
	fmt.Fprint(s.out, args...)
}

func (s *wowSpinner) Error(args ...interface{}) {
	s.Log(fmt.Sprintf("%s %s", s.ErrorPrefix, fmt.Sprint(args...)))
}

func (s *wowSpinner) write(toprint string, prefix string) {
	if prefix != "" {
		s.w.Text(fmt.Sprintf(" %s %s", prefix, toprint))
	} else {
		s.w.Text(fmt.Sprintf(" %s", toprint))
	}
}

func (s *wowSpinner) Stop() {
	s.w.Stop()
}

func (s *wowSpinner) StopWithSuccess(text string) {
	s.Log(fmt.Sprintf("%s %s", s.SuccessPrefix, text))
}

func (s *wowSpinner) StopWithFailure(text string) {
	s.Log(fmt.Sprintf("%s", text))
}

func NewWowSpinner(o io.Writer, options ...func(*wowSpinner)) *wowSpinner {
	w := &wowSpinner{
		out:           o,
		InfoPrefix:    "",
		ErrorPrefix:   "",
		SuccessPrefix: "🌈",
	}

	for _, option := range options {
		option(w)
	}

	return w
}
