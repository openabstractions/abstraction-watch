package watch

import (
	"encoding/binary"
	"errors"
	"os"
	"syscall"
)

const DirsPlatformAPI = "inotify"

// rootDeath is the inotify events that end a watch on the directory itself.
const rootDeath = syscall.IN_DELETE_SELF | syscall.IN_MOVE_SELF | syscall.IN_UNMOUNT | syscall.IN_IGNORED

func Dirs(dirs, _ []string) (<-chan struct{}, func(), func() error, error) {
	if len(dirs) == 0 {
		return nil, nil, nil, errors.New("no watched directory exists yet")
	}
	fd, err := syscall.InotifyInit1(syscall.IN_NONBLOCK | syscall.IN_CLOEXEC)
	if err != nil {
		return nil, nil, nil, err
	}
	added := 0
	for _, dir := range dirs {
		mask := uint32(syscall.IN_CLOSE_WRITE | syscall.IN_MOVED_TO | syscall.IN_CREATE | syscall.IN_DELETE |
			syscall.IN_DELETE_SELF | syscall.IN_MOVE_SELF)
		if _, err := syscall.InotifyAddWatch(fd, dir, mask); err == nil {
			added++
		}
	}
	if added == 0 {
		syscall.Close(fd)
		return nil, nil, nil, errors.New("the directory cannot be watched")
	}
	// Non-blocking, so the runtime polls it and Close unblocks the read that is
	// waiting on it. A descriptor closed under a blocking read does not.
	f := os.NewFile(uintptr(fd), "inotify")
	out := newNotices()
	go func() {
		defer f.Close()
		buf := make([]byte, 4096)
		for {
			n, err := f.Read(buf)
			if err != nil {
				if errors.Is(err, os.ErrClosed) {
					out.end(nil)
				} else {
					out.end(err)
				}
				return
			}
			// Each record is wd, mask, cookie, len, then len bytes of name.
			for off := 0; off+syscall.SizeofInotifyEvent <= n; {
				mask := binary.NativeEndian.Uint32(buf[off+4:])
				size := int(binary.NativeEndian.Uint32(buf[off+12:]))
				if mask&rootDeath != 0 {
					out.end(ErrRootGone)
					return
				}
				off += syscall.SizeofInotifyEvent + size
			}
			out.signal()
		}
	}()
	return out.ch, func() { f.Close() }, out.cause, nil
}
