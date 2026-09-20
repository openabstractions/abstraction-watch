package watch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type followFixture struct {
	next string
	gap  bool
}

func TestFollowGapRestartsFreshSnapshot(t *testing.T) {
	var opens []string
	f := Follow(context.Background(), "old-epoch", func(ctx context.Context, after string) func(func(followFixture, error) bool) {
		opens = append(opens, after)
		return func(yield func(followFixture, error) bool) {
			if after != "" {
				yield(followFixture{next: after, gap: true}, nil)
				return
			}
			yield(followFixture{next: "new-epoch"}, nil)
		}
	}, func(page followFixture) (string, bool) { return page.next, page.gap })
	defer f.Close()
	page, err := f.Next(context.Background())
	if err != nil || page.next != "new-epoch" || page.gap {
		t.Fatal(page, err)
	}
	f.Close()
	if len(opens) != 2 || opens[0] != "old-epoch" || opens[1] != "" {
		t.Fatal(opens)
	}
}

func TestFollowCloseCancelsAndJoinsPresenter(t *testing.T) {
	started, ended := make(chan struct{}), make(chan struct{})
	f := Follow(context.Background(), "", func(ctx context.Context, _ string) func(func(followFixture, error) bool) {
		return func(yield func(followFixture, error) bool) {
			close(started)
			<-ctx.Done()
			defer close(ended)
			yield(followFixture{}, ctx.Err())
		}
	}, func(page followFixture) (string, bool) { return page.next, page.gap })
	<-started
	f.Close()
	select {
	case <-ended:
	default:
		t.Fatal("close returned before source joined")
	}
	if _, e := f.Next(context.Background()); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
}

func TestFollowErrorsEndWithoutTransportRetry(t *testing.T) {
	var calls atomic.Int32
	want := errors.New("disconnected")
	f := Follow(context.Background(), "retained", func(context.Context, string) func(func(followFixture, error) bool) {
		calls.Add(1)
		return func(yield func(followFixture, error) bool) { yield(followFixture{}, want) }
	}, func(page followFixture) (string, bool) { return page.next, page.gap })
	defer f.Close()
	if _, e := f.Next(context.Background()); !errors.Is(e, want) {
		t.Fatal(e)
	}
	if _, e := f.Next(context.Background()); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	if calls.Load() != 1 {
		t.Fatal(calls.Load())
	}
}

func TestFollowCloseReleasesBlockedDelivery(t *testing.T) {
	entered := make(chan struct{})
	f := Follow(context.Background(), "", func(context.Context, string) func(func(followFixture, error) bool) {
		return func(yield func(followFixture, error) bool) {
			close(entered)
			yield(followFixture{next: "snapshot"}, nil)
		}
	}, func(page followFixture) (string, bool) { return page.next, page.gap })
	<-entered
	done := make(chan struct{})
	go func() { f.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close left delivery blocked")
	}
}

func TestFollowFreshSnapshotGapEndsInsteadOfSpinning(t *testing.T) {
	var calls atomic.Int32
	f := Follow(context.Background(), "", func(context.Context, string) func(func(followFixture, error) bool) {
		calls.Add(1)
		return func(yield func(followFixture, error) bool) { yield(followFixture{gap: true}, nil) }
	}, func(page followFixture) (string, bool) { return page.next, page.gap })
	defer f.Close()
	if _, e := f.Next(context.Background()); e == nil {
		t.Fatal("fresh snapshot gap accepted")
	}
	if _, e := f.Next(context.Background()); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	if calls.Load() != 1 {
		t.Fatal("gap retried without a cursor", calls.Load())
	}
}
