package progressbar

import (
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/briandowns/spinner"
	"github.com/schollz/progressbar/v3"

	"github.com/odpf/optimus/utils"
)

const (
	progressBarWidth           = 15
	progressBarRefreshDuration = 120 * time.Millisecond
)

// ProgressBar defines custom progress bar
type ProgressBar struct {
	spinner *spinner.Spinner
	bar     *progressbar.ProgressBar

	mu     sync.Mutex
	writer io.Writer
}

// NewProgressBar initializes default progress bar
func NewProgressBar() *ProgressBar {
	writer := io.Discard
	disableProgressIndicator := strings.ToLower(os.Getenv("OPTIMUS_PROGRESS_INDICATOR"))
	if utils.IsTerminal(os.Stderr) && disableProgressIndicator != "false" {
		writer = os.Stderr
	}
	return NewProgressBarWithWriter(writer)
}

// NewProgressBarWithWriter initializes progress bar with writer
func NewProgressBarWithWriter(w io.Writer) *ProgressBar {
	return &ProgressBar{
		writer: w,
	}
}

// Start starts the progress bar with label
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
	}
	sp := spinner.New(spinner.CharSets[11], progressBarRefreshDuration,
		spinner.WithWriter(p.writer), spinner.WithColor("fgCyan"))
	if label != "" {
		sp.Suffix = " " + label
	}
	sp.Start()
	p.spinner = sp
}

// StartProgress starts progress bar with count and label
func (p *ProgressBar) StartProgress(count int, label string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.bar = progressbar.NewOptions(count,
		progressbar.OptionSetWriter(p.writer),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionShowBytes(false),
		progressbar.OptionSetWidth(progressBarWidth),
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

// Stop stops progress bar
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
