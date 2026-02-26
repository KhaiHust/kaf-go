package config

import "time"

type CommitLogConfig struct {
	SegmentMaxBytes int64         // Max size before rolling to new segment (default: 1GB)
	IndexInterval   int           // Bytes between index entries (default: 4KB)
	RetentionBytes  int64         // Total log size to retain
	RetentionTime   time.Duration // How long to keep segments
}
