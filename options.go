package kv

import "kv2/storage"

const (
	DefaultSegmentSize = 256 * storage.MB
)

type Options struct {
	Dir         string
	SegmentSize int64
}
