package ringbuffer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestRingBuffer_PushAndPop(t *testing.T) {
	rb := NewRingBuffer[int](5, 5*time.Second)

	for i := 1; i <= 3; i++ {
		dropped := rb.Push(i)
		if dropped {
			t.Fatalf("unexpected drop for item %d", i)
		}
	}

	if rb.Len() != 3 {
		t.Fatalf("expected len 3, got %d", rb.Len())
	}

	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		val, err := rb.Pop(ctx)
		if err != nil {
			t.Fatalf("unexpected err: %v", err)
		}
		if val != i {
			t.Fatalf("expected val %d, got %d", i, val)
		}
	}

	if rb.Len() != 0 {
		t.Fatalf("expected len 0, got %d", rb.Len())
	}
}

func TestRingBuffer_OverflowDropOldest(t *testing.T) {
	capacity := 3
	rb := NewRingBuffer[int](capacity, 5*time.Second)

	// Fill to capacity
	rb.Push(10)
	rb.Push(20)
	rb.Push(30)

	if rb.DroppedCount() != 0 {
		t.Fatalf("expected 0 drops so far")
	}

	// 4th push should drop 10
	dropped := rb.Push(40)
	if !dropped {
		t.Fatalf("expected drop on overflow")
	}
	if rb.DroppedCount() != 1 {
		t.Fatalf("expected dropped count 1, got %d", rb.DroppedCount())
	}

	// 5th push should drop 20
	dropped = rb.Push(50)
	if !dropped {
		t.Fatalf("expected drop on overflow")
	}
	if rb.DroppedCount() != 2 {
		t.Fatalf("expected dropped count 2, got %d", rb.DroppedCount())
	}

	// Now buffer should contain [30, 40, 50]
	ctx := context.Background()
	expected := []int{30, 40, 50}
	for _, exp := range expected {
		val, err := rb.Pop(ctx)
		if err != nil {
			t.Fatalf("unexpected pop err: %v", err)
		}
		if val != exp {
			t.Fatalf("expected %d, got %d", exp, val)
		}
	}
}

func TestRingBuffer_ContextCancellation(t *testing.T) {
	rb := NewRingBuffer[string](5, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := rb.Pop(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestRingBuffer_Close(t *testing.T) {
	rb := NewRingBuffer[string](5, 5*time.Second)

	go func() {
		time.Sleep(15 * time.Millisecond)
		rb.Close()
	}()

	_, err := rb.Pop(context.Background())
	if !errors.Is(err, ErrQueueClosed) {
		t.Fatalf("expected ErrQueueClosed, got %v", err)
	}
}

func TestRingBuffer_ConcurrentProducersAndConsumers(t *testing.T) {
	rb := NewRingBuffer[int](20, 5*time.Second)
	numItems := 500

	var wg sync.WaitGroup
	wg.Add(2)

	// Producer
	go func() {
		defer wg.Done()
		for i := 0; i < numItems; i++ {
			rb.Push(i)
			time.Sleep(50 * time.Microsecond)
		}
		rb.Close()
	}()

	// Consumer
	received := 0
	go func() {
		defer wg.Done()
		ctx := context.Background()
		for {
			_, err := rb.Pop(ctx)
			if err != nil {
				break
			}
			received++
		}
	}()

	wg.Wait()

	totalAccounted := received + int(rb.DroppedCount())
	if totalAccounted != numItems {
		t.Fatalf("expected total accounted %d (received %d, dropped %d)", numItems, received, rb.DroppedCount())
	}
}
