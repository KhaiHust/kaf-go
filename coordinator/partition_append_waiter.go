package coordinator

import (
	"time"
)

type PartitionAppendWaiter struct {
	RequiredOffset int64      // last offset of this batch (baseOffset + count - 1)
	ISRSnapshot    []int32    // ISR at append time; used only for logging
	Deadline       time.Time  // now + request.TimeoutMs
	Done           chan error // close with nil on commit, error on timeout/ISR shrink
}
