package skiplist

import "time"

type Options struct {
	DeleteExpired         bool
	DeleteExpiredInterval time.Duration `json:"delete_expired_interval"`
	MaxLevel              int64
	P                     float64
	FlushThreshold        int64
}
