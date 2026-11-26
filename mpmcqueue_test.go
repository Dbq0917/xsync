// Copyright notice. The following tests are partially based on
// the following file from the Go Programming Language core repo:
// https://github.com/golang/go/blob/831f9376d8d730b16fb33dfd775618dffe13ce7a/src/runtime/chan_test.go

package xsync_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/puzpuzpuz/xsync/v4"
)

func TestDeprecatedMPMCQueueOf(t *testing.T) {
	q := NewMPMCQueueOf[int](5)
	if !q.TryEnqueue(1) {
		t.Fatal("enqueue failed")
	}
	if v, ok := q.TryDequeue(); !ok || v != 1 {
		t.Fatalf("got %v/%v, want 1/true", v, ok)
	}
}

func TestMPMCQueueInvalidSize(t *testing.T) {
	defer func() { recover() }()
	NewMPMCQueue[int](0)
	t.Fatal("no panic detected")
}

func TestMPMCQueueInvalidCapacityTooLarge(t *testing.T) {
	defer func() { recover() }()
	NewMPMCQueue[int](int(MpmcQueueMaxRequestedCapacity + 1))
	t.Fatal("no panic detected for overly large capacity")
}

func TestMPMCQueueWraparound(t *testing.T) {
	const capacity = 3
	const cycles = 5
	q := NewMPMCQueue[int](capacity)
	// Cycle through the queue multiple times to test index wraparound
	for cycle := range cycles {
		for i := range capacity {
			if !q.TryEnqueue(cycle*capacity + i) {
				t.Fatalf("cycle %d: enqueue %d failed", cycle, i)
			}
		}
		for i := range capacity {
			v, ok := q.TryDequeue()
			if !ok || v != cycle*capacity+i {
				t.Fatalf("cycle %d: got %v/%v, want %d/true", cycle, v, ok, cycle*capacity+i)
			}
		}
	}
}

func TestMPMCQueueEnqueueDequeueInt(t *testing.T) {
	q := NewMPMCQueue[int](10)
	for i := range 10 {
		if !q.TryEnqueue(i) {
			t.Fatalf("failed to enqueue for %d", i)
		}
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got != i {
			t.Fatalf("%v got %v, want %d", ok, got, i)
		}
	}
}

func TestMPMCQueueEnqueueDequeueString(t *testing.T) {
	q := NewMPMCQueue[string](10)
	for i := range 10 {
		if !q.TryEnqueue(strconv.Itoa(i)) {
			t.Fatalf("failed to enqueue for %d", i)
		}
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got != strconv.Itoa(i) {
			t.Fatalf("%v got %v, want %d", ok, got, i)
		}
	}
}

func TestMPMCQueueEnqueueDequeueStruct(t *testing.T) {
	type foo struct {
		bar int
		baz int
	}
	q := NewMPMCQueue[foo](10)
	for i := range 10 {
		if !q.TryEnqueue(foo{i, i}) {
			t.Fatalf("failed to enqueue for %d", i)
		}
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got.bar != i || got.baz != i {
			t.Fatalf("%v got %v, want %d", ok, got, i)
		}
	}
}

func TestMPMCQueueEnqueueDequeueStructRef(t *testing.T) {
	type foo struct {
		bar int
		baz int
	}
	q := NewMPMCQueue[*foo](11)
	for i := range 10 {
		if !q.TryEnqueue(&foo{i, i}) {
			t.Fatalf("failed to enqueue for %d", i)
		}
	}
	if !q.TryEnqueue(nil) {
		t.Fatal("failed to enqueue for nil")
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got.bar != i || got.baz != i {
			t.Fatalf("%v got %v, want %d", ok, got, i)
		}
	}
	if last, ok := q.TryDequeue(); !ok || last != nil {
		t.Fatalf("%v got %v, want nil", ok, last)
	}
}

func TestMPMCQueueTryEnqueueDequeue(t *testing.T) {
	q := NewMPMCQueue[int](10)
	for i := range 10 {
		if !q.TryEnqueue(i) {
			t.Fatalf("failed to enqueue for %d", i)
		}
	}
	for i := range 10 {
		if got, ok := q.TryDequeue(); !ok || got != i {
			t.Fatalf("got %v, want %d, for status %v", got, i, ok)
		}
	}
}

func TestMPMCQueueTryEnqueueOnFull(t *testing.T) {
	q := NewMPMCQueue[string](1)
	if !q.TryEnqueue("foo") {
		t.Error("failed to enqueue initial item")
	}
	if q.TryEnqueue("bar") {
		t.Error("got success for enqueue on full queue")
	}
}

func TestMPMCQueueTryDequeueOnEmpty(t *testing.T) {
	q := NewMPMCQueue[int](2)
	if _, ok := q.TryDequeue(); ok {
		t.Error("got success for enqueue on empty queue")
	}
}

// Test that capacity is rounded up to the next power of 2.
func TestMPMCQueueCapacityRounding(t *testing.T) {
	tests := []struct {
		requestedCapacity int
		expectedCapacity  int
	}{
		{1, 1},
		{2, 2},
		{3, 4},
		{4, 4},
		{5, 8},
		{10, 16},
		{15, 16},
		{16, 16},
		{17, 32},
	}

	for _, tt := range tests {
		q := NewMPMCQueue[int](tt.requestedCapacity)
		// Try to enqueue exactly the expected (rounded) capacity of items
		for i := 0; i < tt.expectedCapacity; i++ {
			if !q.TryEnqueue(i) {
				t.Fatalf("requestedCapacity=%d: failed to enqueue item %d (expected capacity %d)",
					tt.requestedCapacity, i, tt.expectedCapacity)
			}
		}
		// The queue should now be full, next enqueue should fail
		if q.TryEnqueue(tt.expectedCapacity) {
			t.Fatalf("requestedCapacity=%d: queue should be full after %d items but enqueue succeeded",
				tt.requestedCapacity, tt.expectedCapacity)
		}
		// Dequeue all items to verify they were stored correctly
		for i := 0; i < tt.expectedCapacity; i++ {
			v, ok := q.TryDequeue()
			if !ok {
				t.Fatalf("requestedCapacity=%d: failed to dequeue item %d", tt.requestedCapacity, i)
			}
			if v != i {
				t.Fatalf("requestedCapacity=%d: got %d, want %d", tt.requestedCapacity, v, i)
			}
		}
	}
}

// Test boundary condition: for capacity N, nextPow2(N) items
// should succeed, but nextPow2(N)+1 should fail.
func TestMPMCQueueBoundary(t *testing.T) {
	testCapacities := []int{3, 5, 7, 10, 15, 17}
	expectedRounded := map[int]int{3: 4, 5: 8, 7: 8, 10: 16, 15: 16, 17: 32}

	for _, capacity := range testCapacities {
		q := NewMPMCQueue[int](capacity)
		rounded := expectedRounded[capacity]

		// Enqueue exactly rounded capacity items
		for i := 0; i < rounded; i++ {
			if !q.TryEnqueue(i) {
				t.Fatalf("capacity=%d (rounded to %d): failed to enqueue item %d",
					capacity, rounded, i)
			}
		}

		// Next enqueue should fail (queue is full)
		if q.TryEnqueue(rounded) {
			t.Fatalf("capacity=%d (rounded to %d): enqueue of item %d should have failed (queue is full)",
				capacity, rounded, rounded)
		}

		// Dequeue one item - should succeed
		v, ok := q.TryDequeue()
		if !ok || v != 0 {
			t.Fatalf("capacity=%d (rounded to %d): failed to dequeue from full queue", capacity, rounded)
		}

		// Now we should be able to enqueue one more item
		if !q.TryEnqueue(rounded) {
			t.Fatalf("capacity=%d (rounded to %d): failed to enqueue after dequeue", capacity, rounded)
		}
	}
}

func hammerMPMCQueueNonBlockingCalls(t *testing.T, gomaxprocs, numOps, numThreads int) {
	runtime.GOMAXPROCS(gomaxprocs)
	q := NewMPMCQueue[int](numThreads)
	startwg := sync.WaitGroup{}
	startwg.Add(1)
	csum := make(chan int, numThreads)
	// Start producers.
	for i := range numThreads {
		go func(n int) {
			startwg.Wait()
			for j := n; j < numOps; j += numThreads {
				for !q.TryEnqueue(j) {
					// busy spin until success
				}
			}
		}(i)
	}
	// Start consumers.
	for i := range numThreads {
		go func(n int) {
			startwg.Wait()
			sum := 0
			for j := n; j < numOps; j += numThreads {
				var (
					item int
					ok   bool
				)
				for {
					// busy spin until success
					if item, ok = q.TryDequeue(); ok {
						sum += item
						break
					}
				}
			}
			csum <- sum
		}(i)
	}
	startwg.Done()
	// Wait for all the sums from consumers.
	sum := 0
	for range numThreads {
		s := <-csum
		sum += s
	}
	// Assert the total sum.
	expectedSum := numOps * (numOps - 1) / 2
	if sum != expectedSum {
		t.Fatalf("sums don't match for %d num ops, %d num threads: got %d, want %d",
			numOps, numThreads, sum, expectedSum)
	}
}

func TestMPMCQueueNonBlockingCalls(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 10
	if testing.Short() {
		n = 1
	}
	hammerMPMCQueueNonBlockingCalls(t, 1, n, n)
	hammerMPMCQueueNonBlockingCalls(t, 2, 10*n, 2*n)
	hammerMPMCQueueNonBlockingCalls(t, 4, 100*n, 4*n)
}

func benchmarkMPMCQueue(b *testing.B, queueSize, localWork int) {
	callsPerSched := queueSize
	procs := runtime.GOMAXPROCS(-1) / 2
	if procs == 0 {
		procs = 1
	}
	N := int32(b.N / callsPerSched)
	c := make(chan bool, 2*procs)
	q := NewMPMCQueue[int](queueSize)
	for p := 0; p < procs; p++ {
		go func() {
			foo := 0
			for atomic.AddInt32(&N, -1) >= 0 {
				for range callsPerSched {
					for range localWork {
						foo *= 2
						foo /= 2
					}
					for !q.TryEnqueue(1) {
						runtime.Gosched()
					}
				}
			}
			for !q.TryEnqueue(0) {
				runtime.Gosched()
			}
			c <- foo == 42
		}()
		go func() {
			foo := 0
			for {
				var (
					v  int
					ok bool
				)
				for {
					if v, ok = q.TryDequeue(); !ok {
						runtime.Gosched()
					} else {
						break
					}
				}
				if v == 0 {
					break
				}
				for range localWork {
					foo *= 2
					foo /= 2
				}
			}
			c <- foo == 42
		}()
	}
	for p := 0; p < procs; p++ {
		<-c
		<-c
	}
}

func BenchmarkMPMCQueue(b *testing.B) {
	benchmarkMPMCQueue(b, 1000, 0)
}

func BenchmarkMPMCQueueWork100(b *testing.B) {
	benchmarkMPMCQueue(b, 1000, 100)
}

func waitForMPMCCondition(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(time.Millisecond)
		runtime.Gosched()
	}
	return fn()
}

func waitGroupDone(t *testing.T, wg *sync.WaitGroup, timeout time.Duration, what string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func formatBatchHistogram(hist []atomic.Int64) string {
	var sb strings.Builder
	for size := 1; size < len(hist); size++ {
		if count := hist[size].Load(); count > 0 {
			fmt.Fprintf(&sb, "%d:%d ", size, count)
		}
	}
	return strings.TrimSpace(sb.String())
}

func TestMPMCQueueTryDequeueBatch(t *testing.T) {
	q := NewMPMCQueue[int](8)
	for i := range 5 {
		if !q.TryEnqueue(i) {
			t.Fatalf("enqueue %d failed", i)
		}
	}

	batch := q.TryDequeueBatch(3)
	if len(batch) != 3 {
		t.Fatalf("expected 3 items, got %d", len(batch))
	}
	for i, v := range batch {
		if v != i {
			t.Fatalf("at %d got %d, want %d", i, v, i)
		}
	}

	// max larger than the queue returns the remaining items.
	batch = q.TryDequeueBatch(100)
	if len(batch) != 2 || batch[0] != 3 || batch[1] != 4 {
		t.Fatalf("unexpected remainder: %v", batch)
	}

	if batch := q.TryDequeueBatch(4); len(batch) != 0 {
		t.Fatalf("expected empty batch, got %v", batch)
	}
	if batch := q.TryDequeueBatch(0); batch != nil {
		t.Fatalf("non-positive max should return nil, got %v", batch)
	}

	// Zero-allocation into a caller buffer.
	for i := range 3 {
		if !q.TryEnqueue(i + 10) {
			t.Fatal("enqueue failed")
		}
	}
	buf := make([]int, 2)
	if n := q.TryDequeueInto(buf); n != 2 {
		t.Fatalf("expected 2 into buffer, got %d", n)
	}
	if buf[0] != 10 || buf[1] != 11 {
		t.Fatalf("unexpected buffer contents: %v", buf)
	}
	if n := q.TryDequeueInto(make([]int, 0)); n != 0 {
		t.Fatalf("empty buffer should yield 0, got %d", n)
	}
}

func TestMPMCQueueDequeueBatchDataBeforeCancel(t *testing.T) {
	q := NewMPMCQueue[int](4)
	if !q.TryEnqueue(7) {
		t.Fatal("enqueue failed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Available data is returned even though the context is done.
	batch, err := q.DequeueBatch(ctx, 4)
	if err != nil {
		t.Fatalf("available items should win over canceled ctx, got err %v", err)
	}
	if len(batch) != 1 || batch[0] != 7 {
		t.Fatalf("unexpected batch: %v", batch)
	}
}

func TestMPMCQueueDequeueBatchWake(t *testing.T) {
	q := NewMPMCQueue[int](16)
	type result struct {
		batch []int
		err   error
	}
	resCh := make(chan result, 1)
	go func() {
		batch, err := q.DequeueBatch(context.Background(), 8)
		resCh <- result{batch, err}
	}()

	if !waitForMPMCCondition(2*time.Second, func() bool {
		return GetMPMCQueueWaitStats(q).Waiting == 1
	}) {
		t.Fatalf("consumer did not park: %+v", GetMPMCQueueWaitStats(q))
	}

	for i := range 5 {
		if !q.TryEnqueue(i) {
			t.Fatalf("enqueue %d failed", i)
		}
	}

	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("dequeue failed: %v", res.err)
		}
		if len(res.batch) != 5 {
			t.Fatalf("expected 5 items, got %d", len(res.batch))
		}
		for i, v := range res.batch {
			if v != i {
				t.Fatalf("at %d got %d, want %d", i, v, i)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocking dequeue was not woken by enqueue")
	}

	if stats := GetMPMCQueueWaitStats(q); stats.Waiting != 0 {
		t.Fatalf("waiter leaked after wake: %+v", stats)
	}
}

func TestMPMCQueueDequeueSingle(t *testing.T) {
	q := NewMPMCQueue[string](4)
	go func() {
		time.Sleep(20 * time.Millisecond)
		q.TryEnqueue("hello")
	}()
	v, err := q.Dequeue(context.Background())
	if err != nil || v != "hello" {
		t.Fatalf("got %q/%v", v, err)
	}

	if _, ok := q.DequeueTimeout(30 * time.Millisecond); ok {
		t.Fatal("expected DequeueTimeout to fail on empty queue")
	}
	if !q.TryEnqueue("world") {
		t.Fatal("enqueue failed")
	}
	if v, ok := q.DequeueTimeout(time.Second); !ok || v != "world" {
		t.Fatalf("unexpected DequeueTimeout result: %q/%v", v, ok)
	}
}

func TestMPMCQueueDequeueBatchTimeout(t *testing.T) {
	q := NewMPMCQueue[int](8)

	start := time.Now()
	batch, ok := q.DequeueBatchTimeout(4, 80*time.Millisecond)
	elapsed := time.Since(start)
	if ok || batch != nil {
		t.Fatalf("expected timeout, got batch=%v ok=%v", batch, ok)
	}
	if elapsed < 55*time.Millisecond {
		t.Fatalf("timeout returned too early: %v", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("timeout returned too late: %v", elapsed)
	}

	if batch, ok := q.DequeueBatchTimeout(4, 0); ok || batch != nil {
		t.Fatal("zero timeout on empty queue should fail immediately")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	start = time.Now()
	_, err := q.DequeueBatch(ctx, 4)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("deadline wait duration out of range: %v", elapsed)
	}

	canceledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if _, err := q.DequeueBatch(canceledCtx, 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestMPMCQueueNoWaitersNoNotifyWork(t *testing.T) {
	q := NewMPMCQueue[int](4)
	for i := range 4 {
		if !q.TryEnqueue(i) {
			t.Fatal("enqueue failed")
		}
	}
	if n := q.TryDequeueInto(make([]int, 4)); n != 4 {
		t.Fatalf("expected 4 items, got %d", n)
	}
	stats := GetMPMCQueueWaitStats(q)
	if stats.Waiting != 0 || stats.Notifies != 0 || stats.Wakeups != 0 {
		t.Fatalf("enqueue fast path should do no wake work: %+v", stats)
	}
}

// TestMPMCQueueBatchCancelGhost parks many batch consumers on an empty
// queue, cancels them all, and verifies they return promptly with a zero
// active-waiter count (no ghost waiters) and that the queue stays usable.
func TestMPMCQueueBatchCancelGhost(t *testing.T) {
	q := NewMPMCQueue[int](64)
	const numWaiters = 64

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	errs := make(chan error, numWaiters)
	for range numWaiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := q.DequeueBatch(ctx, 8)
			errs <- err
		}()
	}

	if !waitForMPMCCondition(10*time.Second, func() bool {
		return GetMPMCQueueWaitStats(q).Waiting == numWaiters
	}) {
		t.Fatalf("not all consumers parked: %+v", GetMPMCQueueWaitStats(q))
	}

	cancel()
	waitGroupDone(t, &wg, 20*time.Second, "cancelled waiters to exit")
	close(errs)
	for err := range errs {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled for every waiter, got %v", err)
		}
	}

	if stats := GetMPMCQueueWaitStats(q); stats.Waiting != 0 {
		t.Fatalf("ghost waiters left after cancel: %+v", stats)
	}

	// The queue must remain fully usable after the mass cancellation.
	if !q.TryEnqueue(42) {
		t.Fatal("enqueue after cancellation failed")
	}
	batch, ok := q.DequeueBatchTimeout(8, time.Second)
	if !ok || len(batch) != 1 || batch[0] != 42 {
		t.Fatalf("post-cancellation dequeue got %v/%v", batch, ok)
	}
}

// TestMPMCQueueBatchMPMC is the multi-producer multi-consumer exercise:
// producers publish a unique set of values, batch consumers drain them
// through the blocking API. It checks exactly-once consumption and logs
// the batch-size distribution together with the wait/spin counters.
func TestMPMCQueueBatchMPMC(t *testing.T) {
	const (
		numProducers = 4
		numConsumers = 4
		batchMax     = 32
		capacity     = 256
	)
	numItems := 200_000
	if testing.Short() {
		numItems = 20_000
	}

	q := NewMPMCQueue[int](capacity)
	var sum atomic.Int64
	var count atomic.Int64
	var remaining atomic.Int64
	remaining.Store(int64(numItems))
	hist := make([]atomic.Int64, batchMax+1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var producers sync.WaitGroup
	for p := range numProducers {
		start := p * (numItems / numProducers)
		end := (p + 1) * (numItems / numProducers)
		if p == numProducers-1 {
			end = numItems
		}
		producers.Add(1)
		go func(start, end int) {
			defer producers.Done()
			for v := start; v < end; v++ {
				for !q.TryEnqueue(v) {
					runtime.Gosched()
				}
			}
		}(start, end)
	}

	var consumers sync.WaitGroup
	for range numConsumers {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			for {
				batch, err := q.DequeueBatch(ctx, batchMax)
				if err != nil {
					return
				}
				hist[len(batch)].Add(1)
				count.Add(int64(len(batch)))
				for _, v := range batch {
					sum.Add(int64(v))
				}
				switch rem := remaining.Add(-int64(len(batch))); {
				case rem == 0:
					cancel()
					return
				case rem < 0:
					t.Errorf("batch over-consumed items: remaining=%d", rem)
					cancel()
					return
				}
			}
		}()
	}

	waitGroupDone(t, &producers, 30*time.Second, "producers to finish")
	waitGroupDone(t, &consumers, 30*time.Second, "consumers to stop after completion")

	if got := count.Load(); got != int64(numItems) {
		t.Fatalf("consumed count=%d, want %d", got, numItems)
	}
	if rem := remaining.Load(); rem != 0 {
		t.Fatalf("remaining=%d, want 0", rem)
	}
	wantSum := int64(numItems) * int64(numItems-1) / 2
	if got := sum.Load(); got != wantSum {
		t.Fatalf("consumed sum=%d, want %d", got, wantSum)
	}

	stats := GetMPMCQueueWaitStats(q)
	t.Logf("batch size distribution [size:batches]: %s", formatBatchHistogram(hist))
	t.Logf("wait counters: waiting=%d notifies=%d wakeups=%d emptyPasses(空转)=%d",
		stats.Waiting, stats.Notifies, stats.Wakeups, stats.EmptyPasses)
	if stats.Waiting != 0 {
		t.Fatalf("ghost waiters after MPMC run: %+v", stats)
	}

	multiItemBatches := int64(0)
	distinctSizes := 0
	for size := 1; size <= batchMax; size++ {
		c := hist[size].Load()
		if c > 0 {
			distinctSizes++
			if size > 1 {
				multiItemBatches += c
			}
		}
	}
	if multiItemBatches == 0 {
		t.Fatal("expected at least some multi-item batches, batching did not engage")
	}
	t.Logf("distinct batch sizes=%d, multi-item batches=%d", distinctSizes, multiItemBatches)
}

// TestMPMCQueueBatchTimeoutChurn runs many short-timeout batch calls
// concurrently with producers, stressing cancellation/timeout cleanup
// under the race detector while keeping exact accounting.
func TestMPMCQueueBatchTimeoutChurn(t *testing.T) {
	const (
		numProducers = 4
		itemsPer     = 2500
		numConsumers = 8
		batchMax     = 16
	)
	numItems := int64(numProducers * itemsPer)
	q := NewMPMCQueue[int](128)

	var sum atomic.Int64
	var count atomic.Int64
	remaining := numItems
	var stop atomic.Bool

	var producers sync.WaitGroup
	for p := range numProducers {
		start := p * itemsPer
		end := start + itemsPer
		producers.Add(1)
		go func(start, end int) {
			defer producers.Done()
			for v := start; v < end; v++ {
				for !q.TryEnqueue(v) {
					runtime.Gosched()
				}
			}
		}(start, end)
	}

	var consumers sync.WaitGroup
	for range numConsumers {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			for !stop.Load() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Millisecond)
				batch, err := q.DequeueBatch(ctx, batchMax)
				cancel()
				if err != nil {
					continue
				}
				count.Add(int64(len(batch)))
				for _, v := range batch {
					sum.Add(int64(v))
				}
				if atomic.AddInt64(&remaining, -int64(len(batch))) == 0 {
					stop.Store(true)
				}
			}
		}()
	}

	waitGroupDone(t, &producers, 30*time.Second, "churn producers to finish")
	if !waitForMPMCCondition(20*time.Second, func() bool {
		return count.Load() == numItems
	}) {
		t.Fatalf("did not consume all items: count=%d want=%d", count.Load(), numItems)
	}
	stop.Store(true)
	waitGroupDone(t, &consumers, 10*time.Second, "churn consumers to stop")

	if count.Load() != numItems {
		t.Fatalf("consumed count=%d, want %d (possible duplication/loss)", count.Load(), numItems)
	}
	wantSum := numItems * (numItems - 1) / 2
	if sum.Load() != wantSum {
		t.Fatalf("consumed sum=%d, want %d", sum.Load(), wantSum)
	}
	if stats := GetMPMCQueueWaitStats(q); stats.Waiting != 0 {
		t.Fatalf("waiter count leaked under timeout churn: %+v", stats)
	}
	t.Logf("churn wait counters: %+v", GetMPMCQueueWaitStats(q))
}

func BenchmarkMPMCQueueBatchRoundTrip(b *testing.B) {
	const batch = 16
	q := NewMPMCQueue[int](batch)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for v := range batch {
			if !q.TryEnqueue(v) {
				b.Fatal("enqueue failed")
			}
		}
		got, err := q.DequeueBatch(context.Background(), batch)
		if err != nil || len(got) != batch {
			b.Fatalf("unexpected dequeue: %d/%v", len(got), err)
		}
	}
}

func BenchmarkMPMCQueueTryDequeueInto(b *testing.B) {
	const batch = 16
	q := NewMPMCQueue[int](batch)
	buf := make([]int, batch)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		for v := range batch {
			if !q.TryEnqueue(v) {
				b.Fatal("enqueue failed")
			}
		}
		if n := q.TryDequeueInto(buf); n != batch {
			b.Fatalf("expected %d, got %d", batch, n)
		}
	}
}

// BenchmarkMPMCQueueBatchMPMC transfers b.N items through 4 producers and
// 4 batching/blocking consumers and reports the mean batch size together
// with the empty-pass ("spin") and wake-up rates.
func BenchmarkMPMCQueueBatchMPMC(b *testing.B) {
	const (
		numProducers = 4
		numConsumers = 4
		batchMax     = 32
		capacity     = 1024
	)
	q := NewMPMCQueue[int](capacity)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	total := int64(b.N)
	var consumed atomic.Int64
	var batches atomic.Int64

	var consumers sync.WaitGroup
	for range numConsumers {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			for {
				batch, err := q.DequeueBatch(ctx, batchMax)
				if err != nil {
					return
				}
				batches.Add(1)
				if consumed.Add(int64(len(batch))) == total {
					cancel()
					return
				}
			}
		}()
	}

	b.ResetTimer()
	var producers sync.WaitGroup
	for p := range numProducers {
		start := p * b.N / numProducers
		end := (p + 1) * b.N / numProducers
		producers.Add(1)
		go func(start, end int) {
			defer producers.Done()
			for v := start; v < end; v++ {
				for !q.TryEnqueue(v) {
					runtime.Gosched()
				}
			}
		}(start, end)
	}
	producers.Wait()
	consumers.Wait()
	b.StopTimer()

	if got := consumed.Load(); got != total {
		b.Fatalf("consumed %d, want %d", got, total)
	}
	stats := GetMPMCQueueWaitStats(q)
	b.ReportMetric(float64(total)/float64(batches.Load()), "items/batch")
	b.ReportMetric(float64(stats.EmptyPasses)/float64(b.N), "empty-passes/item")
	b.ReportMetric(float64(stats.Wakeups)/float64(b.N), "wakeups/item")
}
