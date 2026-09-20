package watch

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/windows"
)

const DirsPlatformAPI = "ReadDirectoryChangesW"

// What a watched directory reports, and what its parent reports for it.
const (
	watchedChanges = windows.FILE_NOTIFY_CHANGE_FILE_NAME | windows.FILE_NOTIFY_CHANGE_LAST_WRITE | windows.FILE_NOTIFY_CHANGE_SIZE
	parentChanges  = windows.FILE_NOTIFY_CHANGE_DIR_NAME
)

// A directory read is issued on whichever OS thread runs the goroutine at that
// moment. Without a completion port Windows queues the request to that thread,
// and CancelIoEx does not return while that thread is blocked in unrelated
// synchronous I/O (a Go stdin reader, for example). A handle associated with a
// completion port makes the request independent of the issuing thread, so
// cancellation and completion never wait on it.
//
// FILE_SHARE_DELETE lets a handle follow its directory when it is renamed, and
// a rename produces no change inside the directory. Each watched directory
// therefore keeps its final path from when it was opened, and its parent is
// watched for directory renames: a completion on either that finds the handle
// at another path ends the source with ErrRootGone (WATCH-D1).
type directoryNotification struct {
	mu      sync.Mutex
	h       windows.Handle
	port    windows.Handle
	overlap windows.Overlapped
	buffer  [4096]byte
	filter  uint32
	closed  bool
	done    chan struct{}
	// path is a watched directory's final path when it was opened.
	path string
	// children are the watched directories inside a parent watch, which
	// reports no changes of its own.
	children []*directoryNotification
}

func (d *directoryNotification) arm() error {
	var n uint32
	err := windows.ReadDirectoryChanges(d.h, &d.buffer[0], uint32(len(d.buffer)), false, d.filter, &n, &d.overlap, 0)
	if errors.Is(err, windows.ERROR_IO_PENDING) {
		return nil
	}
	// A synchronous success still queues its completion packet to the port.
	return err
}

// openNotification opens dir and arms its first read. Merely opening a
// directory does not start change notifications.
func openNotification(dir string, filter uint32) (*directoryNotification, error) {
	name, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return nil, err
	}
	h, err := windows.CreateFile(name, windows.FILE_LIST_DIRECTORY,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil,
		windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OVERLAPPED, 0)
	if err != nil {
		return nil, err
	}
	path, err := finalPath(h)
	if err != nil {
		windows.CloseHandle(h)
		return nil, err
	}
	port, err := windows.CreateIoCompletionPort(h, 0, 0, 1)
	if err != nil {
		windows.CloseHandle(h)
		return nil, err
	}
	d := &directoryNotification{h: h, port: port, filter: filter, path: path, done: make(chan struct{})}
	if err := d.arm(); err != nil {
		windows.CloseHandle(h)
		//unchecked: best-effort cleanup on an error path that already discards this directory
		windows.CloseHandle(port)
		return nil, err
	}
	return d, nil
}

func finalPath(h windows.Handle) (string, error) {
	buf := make([]uint16, windows.MAX_PATH)
	for {
		n, err := windows.GetFinalPathNameByHandle(h, &buf[0], uint32(len(buf)), 0)
		if err != nil {
			return "", err
		}
		if int(n) < len(buf) {
			return windows.UTF16ToString(buf[:n]), nil
		}
		buf = make([]uint16, n)
	}
}

// moved reports whether a watched directory's handle now resolves to another
// path. The caller holds d.mu. A handle that cannot be resolved is left to its
// own read, which fails once the directory is gone.
func (d *directoryNotification) moved() bool {
	if d.closed || d.h == 0 {
		return false
	}
	now, err := finalPath(d.h)
	return err == nil && !strings.EqualFold(now, d.path)
}

func Dirs(dirs, _ []string) (<-chan struct{}, func(), func() error, error) {
	if len(dirs) == 0 {
		return nil, nil, nil, errors.New("no watched directory exists yet")
	}
	events := newNotices()
	var open, watched []*directoryNotification
	parents := map[string]*directoryNotification{}
	for _, dir := range dirs {
		// Arm synchronously before the initial configuration snapshot can be read.
		d, err := openNotification(dir, watchedChanges)
		if err != nil {
			continue
		}
		open, watched = append(open, d), append(watched, d)
		go d.report(events)
		parent := filepath.Dir(filepath.Clean(dir))
		if parent == filepath.Clean(dir) {
			continue
		}
		key := strings.ToLower(parent)
		p := parents[key]
		if p == nil {
			// The parent watch only adds move detection; without it a move is
			// still seen at the directory's next change.
			if p, err = openNotification(parent, parentChanges); err != nil {
				continue
			}
			parents[key] = p
			open = append(open, p)
		}
		p.children = append(p.children, d)
	}
	if len(watched) == 0 {
		return nil, nil, nil, errors.New("the directory cannot be opened for notification")
	}
	for _, p := range parents {
		go p.report(events)
	}
	// A move between opening a directory and arming its parent is seen here.
	for _, d := range watched {
		d.mu.Lock()
		moved := d.moved()
		d.mu.Unlock()
		if moved {
			events.end(ErrRootGone)
		}
	}
	var once sync.Once
	return events.ch, func() {
		once.Do(func() {
			for _, d := range open {
				d.mu.Lock()
				d.closed = true
				if d.h != 0 {
					// A pending read completes as aborted; a read already completed
					// leaves nothing to cancel, and report sees closed under the lock.
					windows.CancelIoEx(d.h, nil)
				}
				d.mu.Unlock()
			}
			// A handle and its OVERLAPPED storage remain owned until completion drains.
			for _, d := range open {
				<-d.done
			}
			events.end(nil)
		})
	}, events.cause, nil
}

// report ends the shared notices with a cause when the directory handle fails
// while it is still wanted: a deleted directory completes the pending read with
// an error or refuses the next one, and a moved one resolves to another path.
// A parent watch checks its children on each completion and reports nothing
// itself; when its own handle fails it stops quietly.
func (d *directoryNotification) report(events *notices) {
	defer close(d.done)
	defer func() {
		d.mu.Lock()
		windows.CloseHandle(d.h)
		d.h = 0
		//unchecked: shutdown path with no return value to report a close failure through
		windows.CloseHandle(d.port)
		d.mu.Unlock()
	}()
	parent := d.children != nil
	for {
		var n uint32
		var key uintptr
		var overlap *windows.Overlapped
		err := windows.GetQueuedCompletionStatus(d.port, &n, &key, &overlap, windows.INFINITE)
		if overlap == nil {
			// The port itself failed; no completion can arrive through it.
			d.mu.Lock()
			if !d.closed && !parent {
				events.end(err)
			}
			d.mu.Unlock()
			return
		}
		d.mu.Lock()
		if d.closed {
			d.mu.Unlock()
			return
		}
		if err != nil {
			d.mu.Unlock()
			if !parent {
				events.end(errors.Join(ErrRootGone, err))
			}
			return
		}
		if !parent && d.moved() {
			d.mu.Unlock()
			events.end(ErrRootGone)
			return
		}
		// Rearm while Close is excluded, then expose the notification. Changes
		// after this read are buffered by Windows for the next request.
		err = d.arm()
		d.mu.Unlock()
		if err != nil {
			if !parent {
				events.end(errors.Join(ErrRootGone, err))
			}
			return
		}
		if !parent {
			events.signal()
			continue
		}
		for _, child := range d.children {
			child.mu.Lock()
			moved := child.moved()
			child.mu.Unlock()
			if moved {
				events.end(ErrRootGone)
			}
		}
	}
}
