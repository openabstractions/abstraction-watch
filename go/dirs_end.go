package watch

import (
	"errors"
	"sync"
)

// ErrRootGone is the cause a native source reports when a watched directory is
// deleted, renamed or unmounted. The notification cannot follow the directory,
// so it ends rather than going quiet.
var ErrRootGone = errors.New("the watched directory was removed or moved")

// notices is the one channel a native directory source reports through. It
// coalesces changes and closes exactly once, keeping the first cause. A source
// stopped by its owner closes with a nil cause.
type notices struct {
	mu     sync.Mutex
	ch     chan struct{}
	ended  bool
	reason error
}

func newNotices() *notices { return &notices{ch: make(chan struct{}, 1)} }

func (n *notices) signal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ended {
		return
	}
	select {
	case n.ch <- struct{}{}:
	default:
	}
}

func (n *notices) end(cause error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ended {
		return
	}
	n.ended = true
	n.reason = cause
	close(n.ch)
}

func (n *notices) cause() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.reason
}
