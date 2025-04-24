package debounce

import (
	"sync"
	"time"
)

type Debouncer struct {
	mu     sync.Mutex
	timer  *time.Timer
	after  time.Duration
	action func()
}

func (d *Debouncer) Trigger() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.timer != nil {
		d.timer.Stop()
	}
	d.timer = time.AfterFunc(d.after, d.action)
}

func (d *Debouncer) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.timer != nil {
		d.timer.Stop()
	}
}

func New(after time.Duration, f func()) *Debouncer {
	return &Debouncer{
		after:  after,
		action: f,
	}
}
