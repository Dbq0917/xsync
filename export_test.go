package xsync

import "hash/maphash"

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

func MapBucketIndex[K comparable, V any](m *Map[K, V], key K) uint64 {
	table := m.table.Load()
	var hash uint64
	if m.intKey {
		hash = hashUint64(table.intSeed, toUint64(key))
	} else {
		hash = maphash.Comparable(table.seed, key)
	}
	return uint64(len(table.buckets)-1) & h1(hash)
}
