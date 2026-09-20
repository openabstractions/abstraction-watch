package watch

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MaxWait is the longest wait a service observation accepts (WATCH-W1).
const MaxWait = 30 * time.Second

// DefaultWaiters is the bound a service uses when it has no reason to choose
// another; it leaves connection slots for calls that do not wait.
const DefaultWaiters = 32

// ErrInvalidWait is a wait outside 0..MaxWait. Services answer it as invalid.
var ErrInvalidWait = errors.New("watch: wait outside 0..30000 ms")

// Outcome is what Gate.Wait ended with, in the shared observation vocabulary.
type Outcome int

const (
	// Answered: the probe had something to say, and the recheck allowed it.
	Answered Outcome = iota
	// Unchanged: the wait expired with nothing new.
	Unchanged
	// Busy: every waiter slot was taken; the service answers unavailable
	// (WATCH-W2).
	Busy
	// Failed: the source can no longer observe (WATCH-T1); Gate.Cause says why.
	Failed
)

// Gate is the one server-side waiter every service Observe uses. The service
// owns cursors, epochs, gap rules and authority; the gate owns the wait:
// bounded waiters (WATCH-W2), a generation that wakes every waiter on any
// advance after its read (WATCH-W3), a wait capped at MaxWait (WATCH-W1), a
// recheck after every read and before data returns (WATCH-W4), and one end
// with a cause (WATCH-T1).
type Gate struct {
	mu       sync.Mutex
	advanced chan struct{}
	failed   bool
	cause    error
	slots    chan struct{}
}

// NewGate makes a gate admitting at most waiters concurrent waits.
func NewGate(waiters int) *Gate {
	if waiters <= 0 {
		waiters = DefaultWaiters
	}
	return &Gate{advanced: make(chan struct{}), slots: make(chan struct{}, waiters)}
}

// Advance wakes every current waiter. A waiter that read before the advance
// and has not started waiting yet is woken too.
func (g *Gate) Advance() {
	g.mu.Lock()
	defer g.mu.Unlock()
	close(g.advanced)
	g.advanced = make(chan struct{})
}

// Fail ends the gate with a cause, exactly once (WATCH-T1): every waiter wakes
// and is told Failed, and so is every later Wait. A later Fail keeps the first
// cause and reports false.
func (g *Gate) Fail(cause error) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.failed {
		return false
	}
	g.failed, g.cause = true, cause
	close(g.advanced)
	g.advanced = make(chan struct{})
	return true
}

// Cause is why the gate failed, or nil while it observes.
func (g *Gate) Cause() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.cause
}

// Waiting is how many waits hold a slot now.
func (g *Gate) Waiting() int { return len(g.slots) }

// Failed reports whether the gate has ended.
func (g *Gate) Failed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.failed
}

func (g *Gate) arm() (<-chan struct{}, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.advanced, g.failed
}

// Probe is one observation pass of a service.
//
// Read reads the present against the caller's position and reports whether
// it has something to say; the service keeps what it read in its own closure.
// Recheck confirms the caller may still be answered. It runs after every Read
// and before Wait returns, so an authority withdrawn during the wait returns
// its error and no data (WATCH-W4). Recheck is required.
//
// Deadline, when set, names the next moment the present changes by itself,
// such as an expiry; the wait reads again then (WATCH-H1). The zero time says
// none is known. A deadline crossed during Read/Recheck gets one immediate
// reread; a stale unchanged deadline then waits for progress or the wait end.
type Probe struct {
	Read     func() (answer bool, err error)
	Recheck  func() error
	Deadline func() time.Time
}

// Wait runs probe, and while it has nothing to say waits for an advance, the
// wait's end or ctx. The generation is captured before each Read, so a change
// landing between the read and the wait still wakes it (WATCH-W3). After the
// wait expires Read runs once more; with nothing to say the outcome is
// Unchanged. A wait of zero reads once and never takes a slot.
func (g *Gate) Wait(ctx context.Context, wait time.Duration, probe Probe) (Outcome, error) {
	if wait < 0 || wait > MaxWait {
		return Unchanged, ErrInvalidWait
	}
	if probe.Read == nil || probe.Recheck == nil {
		return Unchanged, errors.New("watch: a probe needs Read and Recheck")
	}
	var timer <-chan time.Time
	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		timer = t.C
	}
	expired := wait == 0
	held := false
	rereadPast := false
	defer func() {
		if held {
			<-g.slots
		}
	}()
	for {
		if e := ctx.Err(); e != nil {
			return Unchanged, e
		}
		select {
		case <-timer:
			expired = true
		default:
		}
		advanced, failed := g.arm()
		if failed {
			return Failed, nil
		}
		readAt := time.Now()
		answer, e := probe.Read()
		if e != nil {
			return Unchanged, e
		}
		if e = probe.Recheck(); e != nil {
			return Unchanged, e
		}
		if answer {
			return Answered, nil
		}
		if expired {
			return Unchanged, nil
		}
		if !held {
			select {
			case g.slots <- struct{}{}:
				held = true
			default:
				return Busy, nil
			}
		}
		var due *time.Timer
		var dueC <-chan time.Time
		if probe.Deadline != nil {
			at := probe.Deadline()
			if at.After(time.Now()) {
				due = time.NewTimer(time.Until(at))
				dueC = due.C
			} else if !rereadPast && at.After(readAt) {
				// The deadline crossed during Read/Recheck. Give the source one
				// immediate pass; a stale unchanged deadline then waits normally.
				rereadPast = true
				continue
			}
		}
		select {
		case <-ctx.Done():
			stop(due)
			return Unchanged, ctx.Err()
		case <-timer:
			expired = true
		case <-advanced:
			rereadPast = false
		case <-dueC:
			rereadPast = false
		}
		stop(due)
	}
}

// Mechanism is how a source learns of change (WATCH-S1): told by the
// platform, asked at an interval, or not at all.
type Mechanism string

const (
	Native Mechanism = "native"
	Polled Mechanism = "polled"
	None   Mechanism = "none"
)

// DirsDeclared is the mechanism Dirs has on this platform.
func DirsDeclared() Mechanism {
	if DirsPlatformAPI == "" {
		return None
	}
	return Native
}
