// Package watch reports the present, and whether it has stopped changing.
//
// A Notice carries what is true now. Quiet says nothing visible has changed
// for at least the budget, and Silence says for how long by this observer's
// clock. The listener's own wait is the only timer: quiet is judged by a
// listener that is waiting, after it has read, so a change the source already
// made is always delivered ahead of a quiet that would postdate it.
//
// Unavailable says the last read of a polled source failed: Now is the last
// good present and Unread is the time since the last successful read. It is
// said once per outage, quiet is never said while reads fail, and the first
// successful read after an outage is said even when its stamp is unchanged.
package watch

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrClosed = errors.New("watch: closed")

const DefaultEvery = time.Second

type Notice[T any] struct {
	Now         T
	Quiet       bool
	Silence     time.Duration
	Unavailable bool
	Unread      time.Duration
}

// Source reads the present and a stamp that differs whenever the present
// visibly differs. An error keeps the last good present rather than replacing
// it with an empty one.
type Source[T any] func() (T, string, error)

// DeadlineSource is a Source that also knows when its present next changes
// by itself, such as a rule's expiry; the zero time says it does not know.
type DeadlineSource[T any] func() (T, string, time.Time, error)

type Subscription[T any] struct {
	read   DeadlineSource[T]
	next   time.Time
	every  time.Duration
	budget time.Duration

	mu      sync.Mutex
	now     T
	stamp   string
	pending bool
	changed time.Time
	told    time.Time
	closed  bool

	lastRead   time.Time
	failing    bool
	toldFailed bool

	posted chan struct{}
	done   chan struct{}
	once   sync.Once
}

// Poll watches a source that has to be asked: it is read on entry to Next and
// then every `every` while a listener waits.
func Poll[T any](read Source[T], every, budget time.Duration) *Subscription[T] {
	return PollDeadline(func() (T, string, time.Time, error) {
		v, stamp, err := read()
		return v, stamp, time.Time{}, err
	}, every, budget)
}

// PollDeadline is Poll for a source that names its next known deadline: the
// subscription reads again at that deadline rather than waiting out the
// interval (WATCH-H1).
func PollDeadline[T any](read DeadlineSource[T], every, budget time.Duration) *Subscription[T] {
	if every <= 0 {
		every = DefaultEvery
	}
	s := newSubscription[T](every, budget)
	s.read = read
	s.refresh()
	s.pending = true
	return s
}

// Push watches a source that says when it moved, through Post.
func Push[T any](first T, stamp string, budget time.Duration) *Subscription[T] {
	s := newSubscription[T](0, budget)
	s.now, s.stamp, s.pending = first, stamp, true
	return s
}

func newSubscription[T any](every, budget time.Duration) *Subscription[T] {
	return &Subscription[T]{
		every:   every,
		budget:  budget,
		changed: time.Now(),
		posted:  make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
}

// Post hands a pushed source's new present in, and reports whether it differed.
func (s *Subscription[T]) Post(v T, stamp string) bool {
	s.mu.Lock()
	changed := s.take(v, stamp)
	s.mu.Unlock()
	if changed {
		select {
		case s.posted <- struct{}{}:
		default:
		}
	}
	return changed
}

func (s *Subscription[T]) take(v T, stamp string) bool {
	if stamp == s.stamp {
		return false
	}
	s.now, s.stamp, s.pending, s.changed = v, stamp, true, time.Now()
	return true
}

func (s *Subscription[T]) refresh() {
	if s.read == nil {
		return
	}
	v, stamp, next, err := s.read()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		// A failing source names no deadline: the interval paces its rereads.
		s.next = time.Time{}
		if !s.failing {
			s.failing, s.toldFailed = true, false
		}
		return
	}
	s.lastRead = time.Now()
	s.next = time.Time{}
	if next.After(s.lastRead) {
		s.next = next // a deadline already passed is ignored, never re-read in a loop
	}
	recovered := s.failing
	s.failing = false
	if !s.take(v, stamp) && recovered {
		s.pending, s.told = true, s.lastRead
	}
}

// Current is the present as of the last read. Next is what reads.
func (s *Subscription[T]) Current() T {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now
}

// Next blocks until there is something to say: a changed present, a quiet one
// once the budget has passed with nothing, an Unavailable one once per outage
// of a polled source (WATCH-U1), ErrClosed, or the context's error.
func (s *Subscription[T]) Next(ctx context.Context) (Notice[T], error) {
	var none Notice[T]
	for {
		s.refresh()
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return none, ErrClosed
		}
		if s.pending {
			s.pending = false
			n := Notice[T]{Now: s.now}
			if s.failing {
				s.toldFailed = true
				n.Unavailable, n.Unread = true, s.unread()
			}
			s.mu.Unlock()
			return n, nil
		}
		if s.failing && !s.toldFailed {
			s.toldFailed = true
			n := Notice[T]{Now: s.now, Unavailable: true, Unread: s.unread()}
			s.mu.Unlock()
			return n, nil
		}
		wait := s.every
		if s.budget > 0 && !s.failing {
			since := s.changed
			if s.told.After(since) {
				since = s.told
			}
			left := s.budget - time.Since(since)
			if left <= 0 {
				s.told = time.Now()
				n := Notice[T]{Now: s.now, Quiet: true, Silence: time.Since(s.changed)}
				s.mu.Unlock()
				return n, nil
			}
			if wait <= 0 || left < wait {
				wait = left
			}
		}
		if left := time.Until(s.next); !s.next.IsZero() && left > 0 {
			// A deadline that is not ahead is never waited for again.
			if wait <= 0 || left < wait {
				wait = left
			}
		}
		s.mu.Unlock()

		var tick <-chan time.Time
		var t *time.Timer
		if wait > 0 {
			t = time.NewTimer(wait)
			tick = t.C
		}
		select {
		case <-ctx.Done():
			stop(t)
			return none, ctx.Err()
		case <-s.done:
			stop(t)
			return none, ErrClosed
		case <-s.posted:
			stop(t)
		case <-tick:
		}
	}
}

// unread is the time since the last successful read; with none yet it is the
// subscription's age.
func (s *Subscription[T]) unread() time.Duration {
	if s.lastRead.IsZero() {
		return time.Since(s.changed)
	}
	return time.Since(s.lastRead)
}

func stop(t *time.Timer) {
	if t != nil {
		t.Stop()
	}
}

// Close ends the subscription: a waiting Next returns ErrClosed, and so does
// every one after it.
func (s *Subscription[T]) Close() error {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		close(s.done)
	})
	return nil
}

// Settle collapses a burst of signals into one call: settled runs once nothing
// has arrived for the budget, and never while signals are still arriving. It
// waits on the channel while nothing is happening, so an idle source costs
// nothing.
//
// This is quiet seen from the source's side. Notice.Quiet tells a waiting
// listener the same fact and repeats it every budget, which is what a listener
// asking "has this stopped?" needs and the opposite of what a debounce needs.
func Settle(ctx context.Context, in <-chan struct{}, budget time.Duration, settled func()) {
	if budget <= 0 {
		budget = DefaultEvery
	}
	timer := time.NewTimer(budget)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	pending := false
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-in:
			if !ok {
				return
			}
			if pending && !timer.Stop() {
				<-timer.C
			}
			timer.Reset(budget)
			pending = true
		case <-timer.C:
			pending = false
			settled()
		}
	}
}
