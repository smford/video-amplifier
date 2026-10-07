package circuitbreaker

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// State represents the state of the circuit breaker.
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateOpen:
		return "OPEN"
	case StateHalfOpen:
		return "HALF-OPEN"
	default:
		return "UNKNOWN"
	}
}

// Config holds circuit breaker tuning parameters.
type Config struct {
	FailureThreshold int           // Consecutive failures before tripping to Open (default: 5)
	BaseDelay        time.Duration // Initial retry delay (default: 1s)
	MaxDelay         time.Duration // Maximum retry delay cap (default: 60s)
	CooldownPeriod   time.Duration // Minimum time to remain Open before Half-Open (default: 30s)
	JitterFraction   float64       // Fraction of delay to jitter, e.g. 0.2 for +/-20% (default: 0.2)
}

// DefaultConfig returns production-ready default circuit breaker settings.
func DefaultConfig() Config {
	return Config{
		FailureThreshold: 5,
		BaseDelay:        1 * time.Second,
		MaxDelay:         30 * time.Second,
		CooldownPeriod:   30 * time.Second,
		JitterFraction:   0.20,
	}
}

// CircuitBreaker guards upstream endpoints and provides exponential backoff with jitter.
type CircuitBreaker struct {
	mu     sync.Mutex
	cfg    Config
	state  State
	rng    *rand.Rand

	consecutiveFailures int
	lastFailureTime     time.Time
	lastSuccessTime     time.Time
	lastError           error
	openUntil           time.Time
}

// New creates a new CircuitBreaker.
func New(cfg Config) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 1 * time.Second
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 60 * time.Second
	}
	if cfg.CooldownPeriod <= 0 {
		cfg.CooldownPeriod = 30 * time.Second
	}
	if cfg.JitterFraction <= 0 || cfg.JitterFraction >= 1.0 {
		cfg.JitterFraction = 0.20
	}

	return &CircuitBreaker{
		cfg:   cfg,
		state: StateClosed,
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Allow reports whether a connection attempt is currently permitted.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		if !now.Before(cb.openUntil) {
			// Cooldown elapsed, enter Half-Open to probe upstream
			cb.state = StateHalfOpen
			return true
		}
		return false
	case StateHalfOpen:
		// In Half-Open, probe in progress
		return true
	default:
		return true
	}
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateOpen && time.Now().After(cb.openUntil) {
		return StateHalfOpen
	}
	return cb.state
}

// ConsecutiveFailures returns the current consecutive failure count.
func (cb *CircuitBreaker) ConsecutiveFailures() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.consecutiveFailures
}

// LastError returns the most recent error.
func (cb *CircuitBreaker) LastError() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.lastError
}

// ReportSuccess resets the circuit breaker to Closed.
func (cb *CircuitBreaker) ReportSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = StateClosed
	cb.consecutiveFailures = 0
	cb.lastError = nil
	cb.lastSuccessTime = time.Now()
}

// ReportFailure records an upstream connection or streaming failure, computes exponential backoff with jitter,
// and trips the circuit to Open if the failure threshold is reached.
// It returns the recommended duration to wait before attempting reconnect.
func (cb *CircuitBreaker) ReportFailure(err error) time.Duration {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	cb.consecutiveFailures++
	cb.lastFailureTime = now
	cb.lastError = err

	// Calculate exponential backoff: base * 2^(failures-1)
	exp := cb.consecutiveFailures - 1
	if exp > 6 {
		exp = 6 // Cap multiplier at 2^6 = 64
	}
	multiplier := 1 << exp
	delay := cb.cfg.BaseDelay * time.Duration(multiplier)
	if delay > cb.cfg.MaxDelay {
		delay = cb.cfg.MaxDelay
	}

	// Apply jitter: +/- (delay * JitterFraction)
	jitterRange := float64(delay) * cb.cfg.JitterFraction
	if jitterRange > 0 {
		jitter := (cb.rng.Float64()*2 - 1) * jitterRange
		delay = time.Duration(float64(delay) + jitter)
		if delay < time.Millisecond {
			delay = time.Millisecond
		}
	}

	if cb.state == StateHalfOpen || cb.consecutiveFailures >= cb.cfg.FailureThreshold {
		cb.state = StateOpen
		cooldown := delay
		if cooldown < cb.cfg.CooldownPeriod {
			cooldown = cb.cfg.CooldownPeriod
		}
		cb.openUntil = now.Add(cooldown)
		return cooldown
	}

	return delay
}

// String returns human-readable diagnostic status.
func (cb *CircuitBreaker) String() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return fmt.Sprintf("state=%s failures=%d last_err=%v", cb.state, cb.consecutiveFailures, cb.lastError)
}
