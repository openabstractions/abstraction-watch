package watch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

const budget = 40 * time.Millisecond

func next[T any](t *testing.T, s *Subscription[T]) Notice[T] {
	t.Helper()
	n, err := s.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestFirstNoticeIsThePresent(t *testing.T) {
	s := Push("a", "1", budget)
	defer s.Close()
	if n := next(t, s); n.Now != "a" || n.Quiet {
		t.Fatalf("first notice = %+v, want the present", n)
	}
}

func TestSeveralChangesCoalesceIntoTheLatest(t *testing.T) {
	s := Push("a", "1", budget)
	defer s.Close()
	next(t, s)
	s.Post("b", "2")
	s.Post("c", "3")
	if n := next(t, s); n.Now != "c" || n.Quiet {
		t.Fatalf("notice = %+v, want the latest", n)
	}
	if n := next(t, s); !n.Quiet {
		t.Fatalf("notice = %+v, want quiet — a backlog was kept", n)
	}
}

func TestTheSameStampIsNotAChange(t *testing.T) {
	s := Push("a", "1", budget)
	defer s.Close()
	next(t, s)
	if s.Post("a", "1") {
		t.Fatal("the same present was reported as a change")
	}
	if n := next(t, s); !n.Quiet {
		t.Fatalf("notice = %+v, want quiet", n)
	}
}

func TestQuietArrivesAfterTheBudgetAndRepeats(t *testing.T) {
	s := Push("a", "1", budget)
	defer s.Close()
	next(t, s)
	first := next(t, s)
	second := next(t, s)
	if !first.Quiet || first.Silence < budget {
		t.Fatalf("first quiet = %+v", first)
	}
	if !second.Quiet || second.Silence < 2*budget {
		t.Fatalf("second quiet = %+v, want one budget after the first", second)
	}
}

func TestAChangeEndsTheSilence(t *testing.T) {
	s := Push("a", "1", budget)
	defer s.Close()
	next(t, s)
	next(t, s)
	s.Post("b", "2")
	if n := next(t, s); n.Quiet || n.Now != "b" {
		t.Fatalf("notice = %+v, want the change", n)
	}
	if n := next(t, s); !n.Quiet || n.Silence < budget || n.Silence > 5*budget {
		t.Fatalf("notice = %+v, want a silence measured from the change", n)
	}
}

func TestAPolledSourceIsReadBeforeSilenceIsJudged(t *testing.T) {
	calls := 0
	read := func() (string, string, error) {
		calls++
		if calls >= 3 {
			return "moved", "2", nil
		}
		return "still", "1", nil
	}
	s := Poll(read, time.Hour, budget)
	defer s.Close()
	next(t, s)
	if n := next(t, s); n.Quiet || n.Now != "moved" {
		t.Fatalf("notice = %+v, want the change the deadline read found", n)
	}
}

func TestAFailingPolledSourceIsUnavailableAndNeverQuiet(t *testing.T) {
	fixture, err := os.Open("testdata/poll-outage.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	var budgetMS, pollMS, outageMS, outages, expected int
	if n, err := fmt.Fscan(fixture, &budgetMS, &pollMS, &outageMS, &outages, &expected); err != nil || n != 5 {
		t.Fatalf("outage fixture: %d fields: %v", n, err)
	}
	var mu sync.Mutex
	failing := false
	read := func() (string, string, error) {
		mu.Lock()
		defer mu.Unlock()
		if failing {
			return "", "", errors.New("share blinked")
		}
		return "a", "1", nil
	}
	s := Poll(read, time.Duration(pollMS)*time.Millisecond, time.Duration(budgetMS)*time.Millisecond)
	defer s.Close()
	next(t, s)
	for outage := 0; outage < outages; outage++ {
		mu.Lock()
		failing = true
		mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(outageMS)*time.Millisecond)
		unavailable := 0
		for {
			n, err := s.Next(ctx)
			if err != nil {
				break
			}
			if n.Quiet {
				t.Fatalf("notice = %+v, want no quiet while reads fail", n)
			}
			if !n.Unavailable || n.Now != "a" {
				t.Fatalf("notice = %+v, want unavailable over the last good present", n)
			}
			unavailable++
		}
		cancel()
		if unavailable != expected {
			t.Fatalf("unavailable notices = %d, want %d", unavailable, expected)
		}
		mu.Lock()
		failing = false
		mu.Unlock()
		if n := next(t, s); n.Unavailable || n.Quiet || n.Now != "a" {
			t.Fatalf("notice = %+v, want a read with the same stamp to clear unavailable", n)
		}
	}
}

func TestClosedEndsAWaitAndEverythingAfterIt(t *testing.T) {
	s := Push("a", "1", 0)
	next(t, s)
	go s.Close()
	if _, err := s.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
	s.Post("b", "2")
	if _, err := s.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v after close, want ErrClosed", err)
	}
}

func TestACancelledContextEndsAWait(t *testing.T) {
	s := Push("a", "1", 0)
	defer s.Close()
	next(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the context's", err)
	}
}

func TestTwoSubscriptionsAreIndependent(t *testing.T) {
	calls := 0
	read := func() (int, string, error) { calls++; return calls, "1", nil }
	a := Poll(read, time.Hour, budget)
	b := Poll(read, time.Hour, budget)
	defer a.Close()
	defer b.Close()
	next(t, a)
	if n := next(t, b); n.Quiet {
		t.Fatal("taking a's present took b's")
	}
}

func TestSettleReportsOneBurstOnce(t *testing.T) {
	in := make(chan struct{}, 8)
	fired := make(chan int, 8)
	calls := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		Settle(context.Background(), in, budget, func() {
			calls++
			fired <- calls
		})
	}()
	for range 5 {
		in <- struct{}{}
	}
	if n := <-fired; n != 1 {
		t.Fatalf("five signals settled %d times", n)
	}
	close(in)
	<-done
	if calls != 1 {
		t.Fatalf("settled %d times for one burst", calls)
	}
}

func TestSettleSaysNothingWhileNothingHappens(t *testing.T) {
	in := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 4*budget)
	defer cancel()
	calls := 0
	Settle(ctx, in, budget, func() { calls++ })
	if calls != 0 {
		t.Fatalf("an idle source settled %d times", calls)
	}
}
