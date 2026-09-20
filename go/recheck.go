package watch

import (
	"context"
	"sync"
	"time"
)

// Recheck is the one asynchronous recheck pass: Request starts a pass off the
// caller's goroutine and returns at once; requests that arrive while a pass
// runs fold into exactly one more pass, so a change after a pass began is seen
// by a pass that starts after it. Each pass runs under ctx and, when budget is
// positive, a per-pass deadline. Stop refuses every later Request, cancels the
// pass in flight and waits for it, so nothing a pass started acts afterwards.
type Recheck struct {
	pass   func(context.Context) error
	budget time.Duration
	ctx    context.Context
	cancel context.CancelFunc

	mu                      sync.Mutex
	running, again, stopped bool
	passes                  sync.WaitGroup
	current, next           *recheckPass
	// After, when set, runs after every pass; tests wait on it.
	After func()
}

type recheckPass struct {
	done chan struct{}
	err  error // published by closing done
}

func newRecheckPass() *recheckPass { return &recheckPass{done: make(chan struct{})} }

// NewRecheck makes a recheck whose passes run under ctx, each bounded by
// budget when it is positive.
func NewRecheck(ctx context.Context, budget time.Duration, pass func(context.Context)) *Recheck {
	return NewCheckedRecheck(ctx, budget, func(ctx context.Context) error { pass(ctx); return nil })
}

// NewCheckedRecheck preserves each pass's result for its requested-pass joins.
// Request remains asynchronous; RequestAndWait returns that pass's error.
func NewCheckedRecheck(ctx context.Context, budget time.Duration, pass func(context.Context) error) *Recheck {
	ctx, cancel := context.WithCancel(ctx)
	return &Recheck{pass: pass, budget: budget, ctx: ctx, cancel: cancel}
}

// Request starts a pass, or marks one more for the pass in flight. After Stop,
// or once ctx ended, it starts nothing.
func (r *Recheck) Request() {
	r.request()
}

// RequestAndWait requests a pass and joins the pass covering this request.
// Requests during a pass join the next coalesced pass; later requests never
// postpone this join. A cancelled or expired pass returns its context error
// only after the pass and its After callback have returned.
func (r *Recheck) RequestAndWait() error {
	pass, err := r.request()
	if err != nil {
		// Stop/context cancellation prevents new passes. An earlier pass or
		// After callback may still act, so even a refused join waits for it.
		r.passes.Wait()
		return err
	}
	<-pass.done
	return pass.err
}

func (r *Recheck) request() (*recheckPass, error) {
	r.mu.Lock()
	if r.stopped || r.ctx.Err() != nil {
		r.mu.Unlock()
		if err := r.ctx.Err(); err != nil {
			return nil, err
		}
		return nil, context.Canceled
	}
	if r.running {
		r.again = true
		if r.next == nil {
			r.next = newRecheckPass()
		}
		pass := r.next
		r.mu.Unlock()
		return pass, nil
	}
	r.running = true
	r.current = newRecheckPass()
	pass := r.current
	r.passes.Add(1)
	r.mu.Unlock()
	go r.loop()
	return pass, nil
}

func (r *Recheck) loop() {
	defer r.passes.Done()
	for {
		r.mu.Lock()
		pass := r.current
		if r.stopped || r.ctx.Err() != nil {
			next := r.next
			r.running, r.again, r.current, r.next = false, false, nil, nil
			r.mu.Unlock()
			pass.err = context.Canceled
			close(pass.done)
			if next != nil {
				next.err = context.Canceled
				close(next.done)
			}
			return
		}
		r.mu.Unlock()
		ctx, cancel := r.ctx, context.CancelFunc(func() {})
		if r.budget > 0 {
			ctx, cancel = context.WithTimeout(r.ctx, r.budget)
		}
		passErr := r.pass(ctx)
		if passErr == nil {
			passErr = ctx.Err()
		}
		cancel()
		r.mu.Lock()
		after := r.After
		more := r.again && !r.stopped && r.ctx.Err() == nil
		next := r.next
		r.running, r.again = more, false
		r.current, r.next = nil, nil
		if more {
			r.current = next
		}
		r.mu.Unlock()
		if after != nil {
			after()
		}
		pass.err = passErr
		close(pass.done)
		if !more && next != nil {
			next.err = context.Canceled
			close(next.done)
		}
		if !more {
			return
		}
	}
}

// Idle reports that no pass is running or requested.
func (r *Recheck) Idle() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.running
}

// Stop refuses later requests, cancels the pass in flight and waits for it.
func (r *Recheck) Stop() {
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	r.cancel()
	r.passes.Wait()
}

// Drain refuses later requests and waits for the pass in flight to finish
// without cancelling it; a host that cancels the pass's context itself first
// uses it.
func (r *Recheck) Drain() {
	r.mu.Lock()
	r.stopped = true
	r.mu.Unlock()
	r.passes.Wait()
}
