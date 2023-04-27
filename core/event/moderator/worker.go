package moderator

import (
	"context"
	"sync"
	"time"

	"github.com/goto/salt/log"
)

type Writer interface {
	Write(messages [][]byte) error
	Close() error
}

type Worker struct {
	mu          sync.Mutex
	wg          sync.WaitGroup
	messageChan <-chan []byte
	closeChan   chan bool

	writer        Writer
	batchInterval time.Duration

	messages [][]byte

	logger log.Logger
}

func NewWorker(messageChan <-chan []byte, writer Writer, batchInterval time.Duration, logger log.Logger) *Worker {
	return &Worker{
		mu:            sync.Mutex{},
		wg:            sync.WaitGroup{},
		messageChan:   messageChan,
		closeChan:     make(chan bool),
		writer:        writer,
		batchInterval: batchInterval,
		logger:        logger,
	}
}

func (w *Worker) Run(ctx context.Context) {
	w.wg.Add(1)
	defer w.wg.Done()

	ticker := time.NewTicker(w.batchInterval)
	for {
		select {
		case msg := <-w.messageChan:
			w.mu.Lock()
			w.messages = append(w.messages, msg)
			w.mu.Unlock()
		case <-ticker.C:
			w.Flush()
		case <-ctx.Done():
			return
		case <-w.closeChan:
			return
		}
	}
}

func (w *Worker) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.messages) == 0 {
		return
	}

	if err := w.writer.Write(w.messages); err != nil {
		w.logger.Error("error writing message: %v", err)
	} else {
		w.messages = nil
	}
}

func (w *Worker) Close() error {
	go func() { w.closeChan <- true }()

	w.wg.Wait()
	w.Flush()
	return w.writer.Close()
}
