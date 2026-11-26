package xsync

import (
	"context"
	"math/bits"
	"sync/atomic"
	"time"
	"unsafe"
)

const mpmcQueueMaxRequestedCapacity uint64 = uint64(1) << (bits.UintSize - 2)

// Deprecated: use [MPMCQueue].
type MPMCQueueOf[I any] = MPMCQueue[I]

// A MPMCQueue is a bounded multi-producer multi-consumer concurrent
// queue.
//
// MPMCQueue instances must be created with NewMPMCQueue function.
// A MPMCQueue must not be copied after first use.
//
// Based on the data structure from the following C++ library:
// https://github.com/rigtorp/MPMCQueue
type MPMCQueue[I any] struct {
	capMask  uint64
	capShift uint64
	// Padding to isolate read-only fields from the head/tail counters.
	_    [cacheLineSize - 16]byte
	head uint64
	// waiting is the active-parked-consumer hint checked on the enqueue
	// fast path. It shares head's cache line, which producers already
	// own for their head CAS, so the check adds no extra cache-line
	// fetch. It is written only when a consumer actually parks (empty
	// queue), never per dequeued item. The following padding keeps tail
	// on its own cache line.
	waiting atomic.Int64
	_       [cacheLineSize - 16]byte
	tail    uint64
	_       [cacheLineSize - 8]byte
	slots   []slotPadded[I]
	// Gate and observability counters live in a separately allocated
	// value, see mpmcWaitState for the alignment rationale. It is nil
	// only for the unusable zero value; NewMPMCQueue always sets it.
	wait *mpmcWaitState
}

type slotPadded[I any] struct {
	slot[I]
	// Unfortunately, proper padding like the below one:
	//
	// pad [cacheLineSize - (unsafe.Sizeof(slot[I]{}) % cacheLineSize)]byte
	//
	// won't compile, so here we add a best-effort padding for items up to
	// 56 bytes size.
	_ [cacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte
}

type slot[I any] struct {
	// atomic.Uint64 is used here to get proper 8 byte alignment on
	// 32-bit archs.
	turn atomic.Uint64
	item I
}

// Deprecated: use [NewMPMCQueue].
func NewMPMCQueueOf[I any](capacity int) *MPMCQueue[I] {
	return NewMPMCQueue[I](capacity)
}

// NewMPMCQueue creates a new MPMCQueue instance with the given
// capacity. The capacity is rounded up to the next power of 2.
func NewMPMCQueue[I any](capacity int) *MPMCQueue[I] {
	if capacity < 1 {
		panic("capacity must be positive number")
	}
	if uint64(capacity) > mpmcQueueMaxRequestedCapacity {
		panic("capacity is too large")
	}
	capPow2 := nextPowOf2(uint64(capacity))
	q := &MPMCQueue[I]{
		capMask:  capPow2 - 1,
		capShift: uint64(bits.TrailingZeros64(capPow2)),
		slots:    make([]slotPadded[I], capPow2),
		wait:     &mpmcWaitState{},
	}
	q.wait.gate.Store(newWakeGate())
	return q
}

// TryEnqueue inserts the given item into the queue. Does not block
// and returns immediately. The result indicates that the queue isn't
// full and the item was inserted.
func (q *MPMCQueue[I]) TryEnqueue(item I) bool {
	head := atomic.LoadUint64(&q.head)
	slot := &q.slots[q.idx(head)]
	turn := q.turn(head) * 2
	if slot.turn.Load() == turn {
		if atomic.CompareAndSwapUint64(&q.head, head, head+1) {
			slot.item = item
			slot.turn.Store(turn + 1)
			// Wake parked batch consumers, if any. This is a single
			// atomic load on the lock-free enqueue fast path and never
			// takes a lock, see notifyWaiters.
			q.notifyWaiters()
			return true
		}
	}
	return false
}

// TryDequeue retrieves and removes the item from the head of the
// queue. Does not block and returns immediately. The ok result
// indicates that the queue isn't empty and an item was retrieved.
func (q *MPMCQueue[I]) TryDequeue() (item I, ok bool) {
	tail := atomic.LoadUint64(&q.tail)
	slot := &q.slots[q.idx(tail)]
	turn := q.turn(tail)*2 + 1
	if slot.turn.Load() == turn {
		if atomic.CompareAndSwapUint64(&q.tail, tail, tail+1) {
			var zeroI I
			item = slot.item
			ok = true
			slot.item = zeroI
			slot.turn.Store(turn + 1)
			return
		}
	}
	return
}

func (q *MPMCQueue[I]) idx(i uint64) uint64 {
	return i & q.capMask
}

func (q *MPMCQueue[I]) turn(i uint64) uint64 {
	return i >> q.capShift
}

// ---------------------------------------------------------------------------
// Batch and blocking dequeue
//
// The ring above stays exactly as it was: producers publish with atomic
// operations and a CAS, never taking a lock. Blocking support is an
// add-on: consumers that find the queue empty register in an atomic
// counter and park on a generation channel (a wakeGate). A producer only
// does the extra gate swap/close when that counter is non-zero.
//
// There is no list of waiting goroutines to unlink: a cancelled or
// timed-out consumer simply returns from select, and its deferred
// counter decrement removes its registration. Hence a cancelled waiter
// cannot be left behind as a ghost node.
// ---------------------------------------------------------------------------

// wakeGate is one wake-up generation. Closing ch broadcasts a wake-up to
// every consumer parked on that gate. The current gate is always open;
// notifyWaiters swaps in a fresh gate before closing the previous one.
type wakeGate struct {
	ch chan struct{}
}

func newWakeGate() *wakeGate {
	return &wakeGate{ch: make(chan struct{})}
}

// mpmcWaitState holds the wake generation and observability counters in a
// separately allocated struct. Leading atomic.Int64 fields stay 8-byte
// aligned on 32-bit architectures where an atomic 64-bit word must be
// 64-bit aligned; a heap-allocated struct's first word provides that.
//
// The hot waiting counter is intentionally kept on MPMCQueue.head's
// cache line (MPMCQueue.waiting) rather than here.
type mpmcWaitState struct {
	// notifies counts producer gate swaps that woke a generation.
	notifies atomic.Int64
	// wakeups counts parked selects that resumed from a closed gate.
	wakeups atomic.Int64
	// emptyPasses counts post-wake retries that still found the queue
	// empty (thundering-herd losers / stale generations), i.e. the
	// non-productive "spin" count of the wait implementation.
	emptyPasses atomic.Int64
	// gate is the current wake generation.
	gate atomic.Pointer[wakeGate]
}

// TryDequeueBatch retrieves and removes up to maxItems items from the
// head of the queue without blocking. Unlike looping over TryDequeue, a
// lost tail CAS is retried internally instead of being mistaken for
// emptiness, so the returned slice is as large as currently possible.
// A zero-length (possibly nil) slice means the queue was empty.
//
// maxItems is capped at the queue capacity. Non-positive values return
// nil.
func (q *MPMCQueue[I]) TryDequeueBatch(maxItems int) []I {
	if maxItems <= 0 {
		return nil
	}
	if capacity := int(q.capMask + 1); maxItems > capacity {
		maxItems = capacity
	}
	// Probe one slot first so an empty queue returns nil without
	// allocating a maxItems-sized buffer (important for polling callers).
	var first [1]I
	if q.drainAvailable(first[:]) == 0 {
		return nil
	}
	buf := make([]I, maxItems)
	buf[0] = first[0]
	n := 1 + q.drainAvailable(buf[1:])
	return buf[:n]
}

// TryDequeueInto fills buf with currently available items, up to
// len(buf), without blocking. It returns the number n of items written
// to buf[:n]. It is the zero-allocation variant of TryDequeueBatch.
func (q *MPMCQueue[I]) TryDequeueInto(buf []I) int {
	if len(buf) == 0 {
		return 0
	}
	return q.drainAvailable(buf)
}

// drainAvailable moves up to len(buf) readily published items into buf.
// It stops at the first not-yet-filled head slot (empty queue) and retries
// when the tail CAS was lost to a concurrent consumer.
func (q *MPMCQueue[I]) drainAvailable(buf []I) int {
	n := 0
	for n < len(buf) {
		tail := atomic.LoadUint64(&q.tail)
		slot := &q.slots[q.idx(tail)]
		turn := q.turn(tail)*2 + 1
		if slot.turn.Load() != turn {
			// Head slot is not published yet: queue is empty.
			return n
		}
		if !atomic.CompareAndSwapUint64(&q.tail, tail, tail+1) {
			// Another consumer won the tail CAS; reload and retry.
			continue
		}
		item := slot.item
		buf[n] = item
		n++
		var zeroI I
		slot.item = zeroI
		slot.turn.Store(turn + 1)
	}
	return n
}

// DequeueInto fills buf with a batch of items, using up to len(buf)
// slots. It first tries the lock-free ring immediately. When the queue is
// empty it parks the goroutine until a producer enqueues an item or ctx
// is cancelled (which includes a deadline/timeout context). At least one
// item is returned whenever n > 0; the batch may be partial.
//
// The returned error is ctx.Err() (context.Canceled or
// context.DeadlineExceeded) when the wait had to be abandoned with no
// item. If items are available they are returned even if ctx is already
// done, so no published item is dropped.
//
// Cancelling ctx wakes the parked goroutine promptly and its waiter
// registration is released before the call returns: there is no
// persistent waiter list, hence no ghost waiters.
func (q *MPMCQueue[I]) DequeueInto(ctx context.Context, buf []I) (int, error) {
	if len(buf) == 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Fast lock-free attempt before registering as a waiter.
	if n := q.drainAvailable(buf); n > 0 {
		return n, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	w := q.wait
	q.waiting.Add(1)
	// Every return after this point releases the registration, so the
	// active-waiter count can never leak on cancellation or timeout.
	defer q.waiting.Add(-1)

	woke := false
	for {
		n := q.drainAvailable(buf)
		if n > 0 {
			return n, nil
		}
		if woke {
			// Previous wake did not yield an item: count the empty pass.
			w.emptyPasses.Add(1)
			woke = false
		}
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		gate := w.gate.Load()
		// Recheck after capturing the gate. This pairs with the
		// producer's swap-before-close and closes the lost-wake window.
		if n := q.drainAvailable(buf); n > 0 {
			return n, nil
		}
		select {
		case <-gate.ch:
			w.wakeups.Add(1)
			woke = true
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// DequeueBatch retrieves a batch of up to maxItems items, blocking while
// the queue is empty as described by DequeueInto. On cancellation or
// timeout with no item it returns a nil slice and ctx.Err().
//
// maxItems is capped at the queue capacity; non-positive values return an
// empty (non-nil) slice and no error without blocking.
func (q *MPMCQueue[I]) DequeueBatch(ctx context.Context, maxItems int) ([]I, error) {
	if maxItems <= 0 {
		return []I{}, nil
	}
	if capacity := int(q.capMask + 1); maxItems > capacity {
		maxItems = capacity
	}
	buf := make([]I, maxItems)
	n, err := q.DequeueInto(ctx, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// DequeueBatchTimeout is the timeout-based convenience wrapper around
// DequeueBatch. The boolean reports whether a batch was retrieved.
//
// A positive timeout bounds the wait when the queue is empty; timeout == 0
// performs a single non-blocking attempt, and a negative timeout waits
// indefinitely. Available items are returned without waiting.
func (q *MPMCQueue[I]) DequeueBatchTimeout(maxItems int, timeout time.Duration) ([]I, bool) {
	switch {
	case timeout < 0:
		items, _ := q.DequeueBatch(context.Background(), maxItems)
		return items, true
	case timeout == 0:
		items := q.TryDequeueBatch(maxItems)
		return items, len(items) > 0
	default:
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		items, err := q.DequeueBatch(ctx, maxItems)
		if err != nil {
			return nil, false
		}
		return items, true
	}
}

// Dequeue retrieves a single item, blocking while the queue is empty. It is
// a convenience wrapper around DequeueBatch with a batch size of one.
func (q *MPMCQueue[I]) Dequeue(ctx context.Context) (item I, err error) {
	buf := make([]I, 1)
	_, err = q.DequeueInto(ctx, buf)
	if err != nil {
		var zeroI I
		return zeroI, err
	}
	return buf[0], nil
}

// DequeueTimeout retrieves a single item, waiting at most timeout. The ok
// result is false on timeout (or a zero timeout with an empty queue).
// A negative timeout waits indefinitely.
func (q *MPMCQueue[I]) DequeueTimeout(timeout time.Duration) (item I, ok bool) {
	items, got := q.DequeueBatchTimeout(1, timeout)
	if !got {
		var zeroI I
		return zeroI, false
	}
	return items[0], true
}

// WaitDequeue blocks until a single item is available and returns it. It
// is the context-free, unbounded-wait variant of Dequeue.
func (q *MPMCQueue[I]) WaitDequeue() I {
	item, _ := q.Dequeue(context.Background())
	return item
}

// WaitDequeueBatch blocks until at least one item is available and then
// returns up to maxItems currently queued items. It is the context-free,
// unbounded-wait variant of DequeueBatch.
func (q *MPMCQueue[I]) WaitDequeueBatch(maxItems int) []I {
	items, _ := q.DequeueBatch(context.Background(), maxItems)
	return items
}

// notifyWaiters wakes one generation of parked consumers after a
// successful enqueue. The waiting counter is checked with a single
// atomic load first, so when no consumer is parked the enqueue path does
// no allocation and takes no lock. With waiters present it publishes a
// fresh gate with a CAS and closes the previous one; the CAS retry makes
// sure a concurrent producer cannot leave a captured gate unclosed.
func (q *MPMCQueue[I]) notifyWaiters() {
	if q.waiting.Load() == 0 {
		return
	}
	w := q.wait
	for {
		old := w.gate.Load()
		fresh := newWakeGate()
		if old == nil {
			if w.gate.CompareAndSwap(nil, fresh) {
				return
			}
			continue
		}
		if w.gate.CompareAndSwap(old, fresh) {
			close(old.ch)
			w.notifies.Add(1)
			return
		}
		// CAS lost to another notifier; reload the current gate.
	}
}
