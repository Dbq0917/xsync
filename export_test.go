package xsync

const (
	EntriesPerMapBucket           = entriesPerMapBucket
	MapLoadFactor                 = mapLoadFactor
	DefaultMinMapTableLen         = defaultMinMapTableLen
	DefaultMinMapTableCap         = defaultMinMapTableLen * entriesPerMapBucket
	MaxMapCounterLen              = maxMapCounterLen
	MpmcQueueMaxRequestedCapacity = mpmcQueueMaxRequestedCapacity
)

type (
	BucketPadded = bucketPadded
)

func EnableAssertions() {
	assertionsEnabled = true
}

func DisableAssertions() {
	assertionsEnabled = false
}

func Cheaprand() uint32 {
	return runtime_cheaprand()
}

func Broadcast(b uint8) uint64 {
	return broadcast(b)
}

func FirstMarkedByteIndex(w uint64) int {
	return firstMarkedByteIndex(w)
}

func MarkZeroBytes(w uint64) uint64 {
	return markZeroBytes(w)
}

func SetByte(w uint64, b uint8, idx int) uint64 {
	return setByte(w, b, idx)
}

func NextPowOf2(v uint64) uint64 {
	return nextPowOf2(v)
}

func HashUint64(seed, v uint64) uint64 {
	return hashUint64(seed, v)
}

// MPMCQueueWaitStats is the white-box snapshot of the blocking batch
// dequeue internals used by tests and benchmarks.
type MPMCQueueWaitStats struct {
	Waiting     int64
	Notifies    int64
	Wakeups     int64
	EmptyPasses int64
}

// GetMPMCQueueWaitStats returns the waiter/spin counters for q.
func GetMPMCQueueWaitStats[I any](q *MPMCQueue[I]) MPMCQueueWaitStats {
	if q.wait == nil {
		return MPMCQueueWaitStats{Waiting: q.waiting.Load()}
	}
	return MPMCQueueWaitStats{
		Waiting:     q.waiting.Load(),
		Notifies:    q.wait.notifies.Load(),
		Wakeups:     q.wait.wakeups.Load(),
		EmptyPasses: q.wait.emptyPasses.Load(),
	}
}
