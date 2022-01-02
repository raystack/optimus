package cmd

import (
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/briandowns/spinner"
	"github.com/schollz/progressbar/v3"
)

type ProgressBar struct {
	spinner *spinner.Spinner
	bar     *progressbar.ProgressBar

	mu     sync.Mutex
	writer io.Writer
}

func NewProgressBarWithWriter(w io.Writer) *ProgressBar {
	return &ProgressBar{
		writer: w,
	}
}

func NewProgressBar() *ProgressBar {
	writer := io.Discard
	disableProgressIndicator := strings.ToLower(os.Getenv("OPTIMUS_PROGRESS_INDICATOR"))
	if isTerminal(os.Stderr) && disableProgressIndicator != "false" {
		writer = os.Stderr
	}
	return NewProgressBarWithWriter(writer)
}

func (p *ProgressBar) Start(label string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.spinner != nil {
		if label == "" {
			p.spinner.Suffix = ""
		} else {
			p.spinner.Suffix = " " + label
		}
		return
	} else {
		sp := spinner.New(spinner.CharSets[11], 120*time.Millisecond,
			spinner.WithWriter(p.writer), spinner.WithColor("fgCyan"))
		if label != "" {
			sp.Suffix = " " + label
		}
		sp.Start()
		p.spinner = sp
	}
}

func (p *ProgressBar) StartProgress(count int, label string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.bar = progressbar.NewOptions(count,
		progressbar.OptionSetWriter(p.writer),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(false),
		progressbar.OptionSetWidth(15),
		progressbar.OptionSetDescription("[cyan] "+label),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionShowCount(),
		progressbar.OptionSetPredictTime(false),
	)
}

// SetProgress sets the progress status
// StartProgress should be called before setting status
func (p *ProgressBar) SetProgress(idx int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.bar == nil {
		return nil
	}
	return p.bar.Set(idx)
}

func (p *ProgressBar) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.spinner != nil {
		p.spinner.Stop()
	}
	if p.bar != nil {
		p.bar.Finish()
		p.bar.Close()
	}
	p.bar = nil
	p.spinner = nil
}
