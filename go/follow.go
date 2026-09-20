package watch

import (
	"context"
	"errors"
)

// Follower receives generated observation pages with bounded backpressure.
// Its owner closes it when the presenter ends. A source must honor its context;
// Close cancels the active request and joins the source before returning.
type Follower[T any] struct {
	pages  chan followPage[T]
	cancel context.CancelFunc
	done   chan struct{}
}

type followPage[T any] struct {
	value T
	err   error
}

// Follow consumes a generated native page sequence. Open binds the context and
// supplied cursor; position reads the typed page's cursor and gap outcome. A gap
// discards its position and opens a fresh snapshot sequence from empty. Typed
// adopters must end their sequence after a refusal. Transport errors are yielded
// once and end this follower; reconnects belong to its owner. No request is
// retried inside a transport failure. Page delivery is unbuffered.
func Follow[T any](ctx context.Context, after string, open func(context.Context, string) func(func(T, error) bool), position func(T) (next string, gap bool)) *Follower[T] {
	ctx, cancel := context.WithCancel(ctx)
	f := &Follower[T]{pages: make(chan followPage[T]), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(f.done)
		defer close(f.pages)
		for ctx.Err() == nil {
			restart := false
			open(ctx, after)(func(page T, err error) bool {
				if ctx.Err() != nil {
					return false
				}
				if err == nil {
					next, gap := position(page)
					if gap {
						if after == "" {
							err = errors.New("watch: gap on fresh snapshot")
						} else {
							after = ""
							restart = true
							return false
						}
					}
					if err == nil {
						after = next
					}
				}
				select {
				case f.pages <- followPage[T]{page, err}:
					return err == nil
				case <-ctx.Done():
					return false
				}
			})
			if !restart {
				return
			}
		}
	}()
	return f
}

// Next waits for one page; cancelling this read leaves ownership with Close.
func (f *Follower[T]) Next(ctx context.Context) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	select {
	case page, ok := <-f.pages:
		if !ok {
			return zero, ErrClosed
		}
		return page.value, page.err
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// Close cancels the native wait and joins it, including a blocked page delivery.
func (f *Follower[T]) Close() { f.cancel(); <-f.done }
