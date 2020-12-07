package models

import (
	"fmt"
	"time"
)

type TaskWindow struct {
	Size       time.Duration
	Offset     time.Duration
	TruncateTo string
}

func (w *TaskWindow) String() string {
	return fmt.Sprintf("size_%dh", int(w.Size.Hours()))
}
