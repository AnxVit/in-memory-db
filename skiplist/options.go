package skiplist

import "time"

type Options struct {
	DeleteExpiredInterval  time.Duration `json:"delete_expired_interval"`
	MemtableMaxLevel       int64
	MemtableP              float64
	MemtableFlushThreshold int64
}
