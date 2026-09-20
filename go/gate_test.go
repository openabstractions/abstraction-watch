package watch

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func allow() error { return nil }

func TestRecheckCompletedPassResultSurvivesStopDuringAfter(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	r := NewCheckedRecheck(t.Context(), time.Second, func(context.Context) error { return nil })
	r.After = func() { close(entered); <-release }
	result, stopped := make(chan error, 1), make(chan struct{})
	go func() { result <- r.RequestAndWait() }()
	<-entered
	go func() { r.Stop(); close(stopped) }()
	select {
	case <-r.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel")
	}
	select {
	case <-result:
		t.Fatal("join escaped After")
	default:
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("completed pass result changed: %v", err)
	}
	<-stopped
}

func TestRecheckCheckedReturnsOnlyItsCoveringPassResult(t *testing.T) {
	first, second := errors.New("first"), errors.New("second")
	entered := make(chan int, 2)
	release := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var n atomic.Int32
	r := NewCheckedRecheck(t.Context(), time.Second, func(context.Context) error {
		i := int(n.Add(1)) - 1
		entered <- i
		<-release[i]
		return []error{first, second}[i]
	})
	t.Cleanup(r.Stop)
	a, b := make(chan error, 1), make(chan error, 1)
	go func() { a <- r.RequestAndWait() }()
	if i := <-entered; i != 0 {
		t.Fatal(i)
	}
	go func() { b <- r.RequestAndWait() }()
	deadline := time.After(time.Second)
	for {
		r.mu.Lock()
		queued := r.next != nil
		r.mu.Unlock()
		if queued {
			break
		}
		select {
		case <-deadline:
			t.Fatal("second request not queued")
		default:
			runtime.Gosched()
		}
	}
	close(release[0])
	if i := <-entered; i != 1 {
		t.Fatal(i)
	}
	select {
	case err := <-a:
		if !errors.Is(err, first) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first waiter delayed by second pass")
	}
	close(release[1])
	if err := <-b; !errors.Is(err, second) {
		t.Fatal(err)
	}
}

// WATCH-W1: a wait that expires with nothing new reads once more and says
// unchanged.
func TestGateExpiryIsUnchanged(t *testing.T) {
	g := NewGate(1)
	var reads atomic.Int32
	start := time.Now()
	out, err := g.Wait(context.Background(), 40*time.Millisecond, Probe{Read: func() (bool, error) { reads.Add(1); return false, nil }, Recheck: allow})
	if err != nil || out != Unchanged {
		t.Fatalf("out=%v err=%v", out, err)
	}
	if time.Since(start) < 40*time.Millisecond || reads.Load() != 2 {
		t.Fatalf("returned after %v with %d reads", time.Since(start), reads.Load())
	}
	if _, err := g.Wait(context.Background(), MaxWait+time.Millisecond, Probe{Read: func() (bool, error) { return false, nil }, Recheck: allow}); !errors.Is(err, ErrInvalidWait) {
		t.Fatalf("over-long wait: %v", err)
	}
}

// A caller that cancels ends its wait at once with the context's error and
// gives its slot back.
func TestGateCancelReturnsAndFreesTheSlot(t *testing.T) {
	g := NewGate(1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := g.Wait(ctx, MaxWait, Probe{Read: func() (bool, error) { return false, nil }, Recheck: allow})
		done <- err
	}()
	waitHeld(t, g, 1)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not end the wait")
	}
	out, _ := g.Wait(context.Background(), 10*time.Millisecond, Probe{Read: func() (bool, error) { return false, nil }, Recheck: allow})
	if out != Unchanged {
		t.Fatalf("slot not returned: %v", out)
	}
}

// A position from a foreign epoch is the service's gap: the probe answers it
// on the wake that follows the epoch change, through the same gate.
func TestGateDeliversGapOnAdvance(t *testing.T) {
	g := NewGate(1)
	var mu sync.Mutex
	epoch, answer := "one", ""
	go func() {
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		epoch = "two"
		mu.Unlock()
		g.Advance()
	}()
	out, err := g.Wait(context.Background(), time.Second, Probe{Read: func() (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		if epoch != "one" {
			answer = "gap"
			return true, nil
		}
		return false, nil
	}, Recheck: allow})
	if err != nil || out != Answered || answer != "gap" {
		t.Fatalf("out=%v err=%v answer=%q", out, err, answer)
	}
}

// WATCH-W3: a change landing after the read and before the wait still wakes.
func TestGateChangeBetweenReadAndWaitWakes(t *testing.T) {
	g := NewGate(1)
	var n atomic.Int32
	start := time.Now()
	out, _ := g.Wait(context.Background(), 5*time.Second, Probe{Read: func() (bool, error) {
		if n.Add(1) == 1 {
			g.Advance() // lands after this read, before the wait
			return false, nil
		}
		return true, nil
	}, Recheck: allow})
	if out != Answered || time.Since(start) > time.Second {
		t.Fatalf("out=%v after %v", out, time.Since(start))
	}
}

// WATCH-W2: the waiter beyond the bound is refused at once.
func TestGateRefusesTheExtraWaiter(t *testing.T) {
	g := NewGate(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go g.Wait(ctx, MaxWait, Probe{Read: func() (bool, error) { return false, nil }, Recheck: allow})
	waitHeld(t, g, 1)
	start := time.Now()
	out, _ := g.Wait(context.Background(), MaxWait, Probe{Read: func() (bool, error) { return false, nil }, Recheck: allow})
	if out != Busy || time.Since(start) > 100*time.Millisecond {
		t.Fatalf("out=%v after %v", out, time.Since(start))
	}
}

// WATCH-W4: authority withdrawn during the wait refuses after the wake and
// returns no data.
func TestGateRechecksAfterEveryWake(t *testing.T) {
	g := NewGate(1)
	var revoked atomic.Bool
	refused := errors.New("forbidden")
	go func() {
		time.Sleep(20 * time.Millisecond)
		revoked.Store(true)
		g.Advance()
	}()
	var rechecks atomic.Int32
	out, err := g.Wait(context.Background(), time.Second, Probe{
		Read: func() (bool, error) { return revoked.Load(), nil },
		Recheck: func() error {
			rechecks.Add(1)
			if revoked.Load() {
				return refused
			}
			return nil
		}})
	if !errors.Is(err, refused) || out == Answered || rechecks.Load() != 2 {
		t.Fatalf("out=%v err=%v rechecks=%d", out, err, rechecks.Load())
	}
}

// WATCH-T1: a source that can no longer observe fails exactly once, with its
// cause, and wakes every waiter.
func TestGateFailsExactlyOnce(t *testing.T) {
	g := NewGate(2)
	gone := errors.New("root gone")
	done := make(chan Outcome, 1)
	go func() {
		out, _ := g.Wait(context.Background(), MaxWait, Probe{Read: func() (bool, error) { return false, nil }, Recheck: allow})
		done <- out
	}()
	waitHeld(t, g, 1)
	if !g.Fail(gone) || g.Fail(errors.New("later")) {
		t.Fatal("Fail did not report exactly one end")
	}
	if out := <-done; out != Failed || !errors.Is(g.Cause(), gone) {
		t.Fatalf("out=%v cause=%v", out, g.Cause())
	}
}

// One recheck helper: requests during a pass fold into one more pass, and
// Stop cancels the pass in flight and waits for it.
func TestRecheckCoalescesAndStops(t *testing.T) {
	release := make(chan struct{})
	var passes atomic.Int32
	r := NewRecheck(context.Background(), time.Second, func(ctx context.Context) {
		if passes.Add(1) == 1 {
			<-release
		}
	})
	idle := make(chan struct{}, 4)
	r.After = func() { idle <- struct{}{} }
	r.Request()
	time.Sleep(10 * time.Millisecond)
	r.Request()
	r.Request()
	r.Request()
	close(release)
	<-idle
	<-idle
	if passes.Load() != 2 {
		t.Fatalf("passes = %d, want 2", passes.Load())
	}
	blocked := make(chan struct{})
	r2 := NewRecheck(context.Background(), 0, func(ctx context.Context) { close(blocked); <-ctx.Done() })
	r2.Request()
	<-blocked
	r2.Stop()
	r2.Request()
	r.Stop()
}

// WATCH-H1: a source that names its next deadline is read again at that
// deadline, not at the next interval.
func TestPollWakesAtTheSourcesDeadline(t *testing.T) {
	expiry := time.Now().Add(80 * time.Millisecond)
	s := PollDeadline(func() (string, string, time.Time, error) {
		if time.Now().Before(expiry) {
			return "granted", "1", expiry, nil
		}
		return "expired", "2", time.Time{}, nil
	}, 10*time.Second, 0)
	defer s.Close()
	if n, _ := s.Next(context.Background()); n.Now != "granted" {
		t.Fatalf("first = %q", n.Now)
	}
	n, err := s.Next(context.Background())
	if err != nil || n.Now != "expired" || time.Since(expiry) > 50*time.Millisecond {
		t.Fatalf("n=%v err=%v late by %v", n.Now, err, time.Since(expiry))
	}
}

// Each pass runs under the per-pass budget.
func TestRecheckPassHasADeadline(t *testing.T) {
	got := make(chan error, 1)
	r := NewRecheck(context.Background(), 20*time.Millisecond, func(ctx context.Context) {
		<-ctx.Done()
		got <- ctx.Err()
	})
	r.Request()
	select {
	case err := <-got:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pass had no deadline")
	}
	r.Stop()
}

// A join covers its own generation, including a coalesced next pass, while
// later unrelated requests may keep the helper busy after that join returns.
func TestRecheckRequestAndWaitJoinsItsRequestedPass(t *testing.T) {
	entered := make(chan int, 3)
	release := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	var calls atomic.Int32
	r := NewRecheck(context.Background(), time.Second, func(ctx context.Context) {
		n := int(calls.Add(1)) - 1
		entered <- n
		select {
		case <-release[n]:
		case <-ctx.Done():
		}
	})
	defer r.Stop()
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { first <- r.RequestAndWait() }()
	<-entered
	go func() { second <- r.RequestAndWait() }()
	deadline := time.Now().Add(time.Second)
	for {
		r.mu.Lock()
		queued := r.next != nil
		r.mu.Unlock()
		if queued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second request did not queue its pass")
		}
		time.Sleep(time.Millisecond)
	}
	r.Request() // folds into the second request's pass
	close(release[0])
	<-entered
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	select {
	case <-second:
		t.Fatal("second join escaped before its pass finished")
	default:
	}
	r.Request() // a later request needs a third pass
	close(release[1])
	<-entered
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("later request postponed the completed second join")
	}
	close(release[2])
}

func TestRecheckRequestAndWaitJoinsCancellationAndAfter(t *testing.T) {
	for _, end := range []string{"stop", "owner cancellation"} {
		t.Run(end, func(t *testing.T) {
			owner, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, cancelled, after := make(chan struct{}), make(chan struct{}), make(chan struct{})
			release, releaseAfter := make(chan struct{}), make(chan struct{})
			var passOnce, afterOnce sync.Once
			defer passOnce.Do(func() { close(release) })
			defer afterOnce.Do(func() { close(releaseAfter) })
			r := NewRecheck(owner, 0, func(ctx context.Context) {
				close(entered)
				<-ctx.Done()
				close(cancelled)
				<-release // the cancelled pass can still act until it returns
			})
			r.After = func() { close(after); <-releaseAfter }
			joined := make(chan error, 1)
			go func() { joined <- r.RequestAndWait() }()
			<-entered
			stopped := make(chan struct{})
			go func() {
				if end == "stop" {
					r.Stop()
				} else {
					cancel()
				}
				close(stopped)
			}()
			<-cancelled
			select {
			case <-joined:
				t.Fatal("cancelled join escaped an active pass")
			default:
			}
			passOnce.Do(func() { close(release) })
			<-after
			select {
			case <-joined:
				t.Fatal("join escaped its After callback")
			default:
			}
			afterOnce.Do(func() { close(releaseAfter) })
			if err := <-joined; !errors.Is(err, context.Canceled) {
				t.Fatalf("joined error: %v", err)
			}
			<-stopped
			if err := r.RequestAndWait(); !errors.Is(err, context.Canceled) {
				t.Fatalf("request after cancellation: %v", err)
			}
			r.Stop()
		})
	}
}

func TestRecheckRequestAndWaitReportsPassDeadline(t *testing.T) {
	r := NewRecheck(context.Background(), 10*time.Millisecond, func(ctx context.Context) { <-ctx.Done() })
	defer r.Stop()
	if err := r.RequestAndWait(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired pass reported successful settlement: %v", err)
	}
}

// WATCH-H1 for a service wait: a probe that names an expiry is read again at
// it, without an advance.
func TestGateWakesAtTheProbesDeadline(t *testing.T) {
	g := NewGate(1)
	expiry := time.Now().Add(60 * time.Millisecond)
	out, err := g.Wait(context.Background(), 5*time.Second, Probe{
		Read:     func() (bool, error) { return !time.Now().Before(expiry), nil },
		Recheck:  allow,
		Deadline: func() time.Time { return expiry },
	})
	if err != nil || out != Answered || time.Since(expiry) > 200*time.Millisecond {
		t.Fatalf("out=%v err=%v late by %v", out, err, time.Since(expiry))
	}
}

// waitHeld polls until n waits hold a slot, so a test never races the first
// waiter with a sleep.
func waitHeld(t *testing.T, g *Gate, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for g.Waiting() < n {
		if time.Now().After(deadline) {
			t.Fatalf("waiters = %d, want %d", g.Waiting(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// WATCH-H1: a deadline already passed is ignored, so a probe that keeps
// naming it is not re-read every millisecond until the wait ends.
func TestGatePastDeadlineDoesNotSpin(t *testing.T) {
	g := NewGate(1)
	var reads atomic.Int32
	past := time.Now().Add(-time.Second)
	out, _ := g.Wait(context.Background(), 100*time.Millisecond, Probe{
		Read:     func() (bool, error) { reads.Add(1); return false, nil },
		Recheck:  allow,
		Deadline: func() time.Time { return past },
	})
	if out != Unchanged || reads.Load() > 3 {
		t.Fatalf("out=%v reads=%d", out, reads.Load())
	}
}

func TestGateDeadlineCrossingRecheckReadsAgain(t *testing.T) {
	g := NewGate(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var expiry time.Time
	entered, release := make(chan struct{}), make(chan struct{})
	var checks atomic.Int32
	var reads int
	done := make(chan Outcome, 1)
	go func() {
		outcome, err := g.Wait(ctx, time.Second, Probe{
			Read: func() (bool, error) {
				reads++
				if reads == 1 {
					expiry = time.Now().Add(30 * time.Millisecond)
					return false, nil
				}
				return !time.Now().Before(expiry), nil
			},
			Recheck: func() error {
				if checks.Add(1) == 1 {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			},
			Deadline: func() time.Time { return expiry },
		})
		if err != nil && ctx.Err() == nil {
			t.Error(err)
		}
		done <- outcome
	}()
	<-entered
	timer := time.NewTimer(time.Until(expiry))
	<-timer.C
	close(release)
	select {
	case outcome := <-done:
		if outcome != Answered {
			t.Fatal(outcome)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("deadline crossed recheck but was not reread")
	}
}

func TestGateCrossedStaleDeadlineDoesNotSpinOrExtendWait(t *testing.T) {
	g := NewGate(1)
	expiry := time.Now().Add(10 * time.Millisecond)
	var reads, checks atomic.Int32
	start := time.Now()
	outcome, err := g.Wait(context.Background(), 50*time.Millisecond, Probe{
		Read: func() (bool, error) { reads.Add(1); return false, nil },
		Recheck: func() error {
			if checks.Add(1) == 1 {
				timer := time.NewTimer(time.Until(expiry))
				<-timer.C
			}
			return nil
		},
		Deadline: func() time.Time { return expiry },
	})
	if err != nil || outcome != Unchanged || reads.Load() > 3 || time.Since(start) < 50*time.Millisecond {
		t.Fatal(outcome, err, reads.Load(), time.Since(start))
	}
}

// WATCH-H1: a polled source that keeps naming a passed deadline is read at
// its interval, not every millisecond.
func TestPollDeadlinePastDeadlineDoesNotSpin(t *testing.T) {
	var reads atomic.Int32
	past := time.Now().Add(-time.Second)
	s := PollDeadline(func() (int, string, time.Time, error) { reads.Add(1); return 1, "a", past, nil }, time.Hour, 0)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	s.Next(ctx) // the snapshot
	s.Next(ctx) // waits until ctx ends
	if reads.Load() > 3 {
		t.Fatalf("reads=%d", reads.Load())
	}
}

// WATCH-W2: a wait whose first read answers takes no slot, so it is answered
// while every slot is held.
func TestGateAnsweredReadTakesNoSlot(t *testing.T) {
	g := NewGate(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	held := make(chan struct{})
	go func() {
		g.Wait(ctx, MaxWait, Probe{Read: func() (bool, error) { return false, nil }, Recheck: allow})
		close(held)
	}()
	for deadline := time.Now().Add(3 * time.Second); g.Waiting() != 1; {
		if time.Now().After(deadline) {
			t.Fatal("the first waiter took no slot")
		}
		runtime.Gosched()
	}
	out, err := g.Wait(context.Background(), MaxWait, Probe{Read: func() (bool, error) { return true, nil }, Recheck: allow})
	if out != Answered || err != nil {
		t.Fatalf("out=%v err=%v", out, err)
	}
	cancel()
	<-held
}

// WATCH-H1 with WATCH-U1: a polled source that names a near deadline and then
// fails is reread at its interval once the deadline passes, not every
// millisecond.
func TestPollDeadlineFailingAfterDeadlineDoesNotSpin(t *testing.T) {
	var reads atomic.Int32
	soon := time.Now().Add(20 * time.Millisecond)
	s := PollDeadline(func() (int, string, time.Time, error) {
		if reads.Add(1) == 1 {
			return 1, "a", soon, nil
		}
		return 0, "", time.Time{}, errors.New("outage")
	}, time.Hour, 0)
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	for {
		if _, err := s.Next(ctx); err != nil {
			break
		}
	}
	if reads.Load() > 5 {
		t.Fatalf("reads=%d", reads.Load())
	}
}

// WATCH-S1: the declared mechanism of Dirs matches the platform API it uses,
// and goes red when DirsDeclared misreports.
func TestDirsDeclaredMatchesThePlatform(t *testing.T) {
	want := Native
	if DirsPlatformAPI == "" {
		want = None
	}
	if got := DirsDeclared(); got != want {
		t.Fatalf("DirsDeclared() = %q with platform API %q", got, DirsPlatformAPI)
	}
	switch runtime.GOOS {
	case "windows", "linux", "darwin", "freebsd", "openbsd", "netbsd", "dragonfly":
		if DirsDeclared() != Native {
			t.Fatalf("%s must declare native directory notification", runtime.GOOS)
		}
	}
}
