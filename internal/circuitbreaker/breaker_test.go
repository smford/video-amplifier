package circuitbreaker

import (
	"errors"
	"testing"
	"time"
)

func TestCircuitBreaker_StateTransitions(t *testing.T) {
	cfg := Config{
		FailureThreshold: 3,
		BaseDelay:        10 * time.Millisecond,
		MaxDelay:         100 * time.Millisecond,
		CooldownPeriod:   50 * time.Millisecond,
		JitterFraction:   0.1,
	}

	cb := New(cfg)

	// Initially closed and allowed
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed, got %v", cb.State())
	}
	if !cb.Allow() {
		t.Fatalf("expected Allow() true in closed state")
	}

	// 1st failure
	delay1 := cb.ReportFailure(errors.New("conn reset"))
	if cb.State() != StateClosed {
		t.Fatalf("expected still Closed after 1 failure, got %v", cb.State())
	}
	if delay1 < 8*time.Millisecond || delay1 > 15*time.Millisecond {
		t.Logf("delay1 = %v", delay1)
	}

	// 2nd failure
	_ = cb.ReportFailure(errors.New("timeout"))
	if cb.State() != StateClosed {
		t.Fatalf("expected still Closed after 2 failures, got %v", cb.State())
	}

	// 3rd failure (trips threshold)
	cooldown := cb.ReportFailure(errors.New("host unreachable"))
	if cb.State() != StateOpen {
		t.Fatalf("expected StateOpen after 3 failures, got %v", cb.State())
	}
	if cooldown < 50*time.Millisecond {
		t.Fatalf("expected cooldown >= 50ms, got %v", cooldown)
	}

	// Immediate Allow() should be false while Open
	if cb.Allow() {
		t.Fatalf("expected Allow() false while in Open state before cooldown")
	}

	// Wait for cooldown to expire
	time.Sleep(60 * time.Millisecond)

	// Now should transition to Half-Open and allow probe
	if !cb.Allow() {
		t.Fatalf("expected Allow() true after cooldown passed")
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected StateHalfOpen, got %v", cb.State())
	}

	// Successful probe should reset to Closed
	cb.ReportSuccess()
	if cb.State() != StateClosed {
		t.Fatalf("expected StateClosed after success, got %v", cb.State())
	}
	if cb.ConsecutiveFailures() != 0 {
		t.Fatalf("expected 0 failures after success, got %d", cb.ConsecutiveFailures())
	}
}

func TestCircuitBreaker_HalfOpenFailure(t *testing.T) {
	cfg := Config{
		FailureThreshold: 1,
		BaseDelay:        5 * time.Millisecond,
		MaxDelay:         50 * time.Millisecond,
		CooldownPeriod:   20 * time.Millisecond,
		JitterFraction:   0.05,
	}

	cb := New(cfg)
	_ = cb.ReportFailure(errors.New("fail 1"))
	if cb.State() != StateOpen {
		t.Fatalf("expected Open, got %v", cb.State())
	}

	time.Sleep(25 * time.Millisecond)
	if !cb.Allow() {
		t.Fatalf("expected Allow() true in half-open")
	}

	// Failing in half-open immediately returns to Open
	_ = cb.ReportFailure(errors.New("probe failed"))
	if cb.State() != StateOpen {
		t.Fatalf("expected Open after half-open failure, got %v", cb.State())
	}
}
