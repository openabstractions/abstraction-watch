//go:build darwin || freebsd || netbsd || openbsd || dragonfly

package watch

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const DirsPlatformAPI = "kqueue"

func Dirs(dirs, files []string) (<-chan struct{}, func(), func() error, error) {
	if len(dirs) == 0 {
		return nil, nil, nil, errors.New("no watched directory exists yet")
	}
	kq, err := syscall.Kqueue()
	if err != nil {
		return nil, nil, nil, err
	}
	syscall.CloseOnExec(kq)
	var wake [2]int
	if err := syscall.Pipe(wake[:]); err != nil {
		syscall.Close(kq)
		return nil, nil, nil, err
	}
	syscall.CloseOnExec(wake[0])
	syscall.CloseOnExec(wake[1])
	type vnode struct {
		file *os.File
		info os.FileInfo
	}
	opened := map[string]vnode{}
	cleanup := func() {
		syscall.Close(kq)
		syscall.Close(wake[0])
		for _, node := range opened {
			node.file.Close()
		}
	}
	fail := func(err error) (<-chan struct{}, func(), func() error, error) {
		cleanup()
		syscall.Close(wake[1])
		return nil, nil, nil, err
	}
	add := func(path string) error {
		// Nonblocking open also prevents a replaced path that became a FIFO from
		// trapping the notification goroutine. Only regular files/directories qualify.
		fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			return err
		}
		syscall.CloseOnExec(fd)
		f := os.NewFile(uintptr(fd), path)
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			f.Close()
			return nil
		}
		var ev syscall.Kevent_t
		syscall.SetKevent(&ev, fd, syscall.EVFILT_VNODE, syscall.EV_ADD|syscall.EV_CLEAR)
		ev.Fflags = syscall.NOTE_WRITE | syscall.NOTE_EXTEND | syscall.NOTE_ATTRIB | syscall.NOTE_RENAME | syscall.NOTE_DELETE | syscall.NOTE_REVOKE
		if _, err := syscall.Kevent(kq, []syscall.Kevent_t{ev}, nil, nil); err != nil {
			f.Close()
			return err
		}
		opened[path] = vnode{f, info}
		return nil
	}
	// Directory notifications detect names appearing/disappearing. An in-place
	// write affects the file vnode, so subscribe to each config file as well.
	var paths []string
	watched := map[string]bool{}
	for _, dir := range dirs {
		if watched[dir] {
			continue
		}
		if err := add(dir); err != nil {
			continue
		}
		if _, ok := opened[dir]; !ok {
			continue
		}
		watched[dir] = true
		paths = append(paths, dir)
	}
	if len(opened) == 0 {
		return fail(errors.New("the directory cannot be watched"))
	}
	for _, file := range files {
		if watched[filepath.Dir(file)] {
			paths = append(paths, file)
		}
	}
	refresh := func() error {
		var failures []error
		for _, path := range paths {
			info, err := os.Stat(path)
			old, exists := opened[path]
			// A watched directory that is missing or replaced ends the source.
			// Closing its descriptor would drop the pending kqueue root event and
			// leave the loop waiting on the wake pipe alone.
			if watched[path] && (errors.Is(err, os.ErrNotExist) || (err == nil && exists && !os.SameFile(info, old.info))) {
				return ErrRootGone
			}
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				failures = append(failures, err)
				continue
			}
			if err == nil && exists && os.SameFile(info, old.info) {
				continue
			}
			if exists {
				old.file.Close()
				delete(opened, path)
			}
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			// A rename/unlink may race the open; its directory event will retry.
			if err := add(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				failures = append(failures, err)
			}
		}
		return errors.Join(failures...)
	}
	if err := refresh(); err != nil {
		return fail(err)
	}
	var stopper syscall.Kevent_t
	syscall.SetKevent(&stopper, wake[0], syscall.EVFILT_READ, syscall.EV_ADD)
	if _, err := syscall.Kevent(kq, []syscall.Kevent_t{stopper}, nil, nil); err != nil {
		return fail(err)
	}
	// A watched directory's own descriptor ends the notification when the
	// directory is deleted, renamed or revoked: it can no longer be followed.
	// Roots are looked up from the current descriptors: a closed fd number
	// may be reused by a reopened config file.
	isRoot := func(fd int) bool {
		for dir := range watched {
			if node, ok := opened[dir]; ok && int(node.file.Fd()) == fd {
				return true
			}
		}
		return false
	}
	events := newNotices()
	done := make(chan struct{})
	var end sync.Once
	signalStop := func() { end.Do(func() { syscall.Close(wake[1]) }) }
	go func() {
		defer close(done)
		var cause error
		defer func() { events.end(cause) }()
		defer cleanup()
		defer signalStop()
		out := make([]syscall.Kevent_t, len(paths)+1)
		for {
			n, err := syscall.Kevent(kq, nil, out, nil)
			if err == syscall.EINTR {
				continue
			}
			if err != nil {
				cause = err
				return
			}
			for _, ev := range out[:n] {
				if int(ev.Ident) == wake[0] {
					return
				}
				if isRoot(int(ev.Ident)) && ev.Fflags&(syscall.NOTE_DELETE|syscall.NOTE_RENAME|syscall.NOTE_REVOKE) != 0 {
					cause = ErrRootGone
					return
				}
			}
			if n > 0 {
				// Rearm replacement inodes before announcing the edit: a subsequent save
				// must not be lost while the subscriber consumes the previous notice.
				if err := refresh(); errors.Is(err, ErrRootGone) {
					cause = ErrRootGone
					return
				} else if err != nil {
					// Retain other live watches. Another native event can retry an
					// inaccessible path; do not silently stop or switch to polling.
					fmt.Fprintf(os.Stderr, "abstraction: kqueue rearm: %v\n", err)
				}
				events.signal()
			}
		}
	}()
	return events.ch, func() { signalStop(); <-done }, events.cause, nil
}
