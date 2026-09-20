package watch

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestDirectoryNotificationIsArmedBeforeReturn(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	dir := t.TempDir()
	events, stop, _, err := Dirs([]string{dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	// With one P, write immediately, before a newly launched reporter can arm.
	if err := os.WriteFile(filepath.Join(dir, "external.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("notification was not armed when Dirs returned")
	}
}

func TestDirectoryNotificationCloseWithoutWriter(t *testing.T) {
	for i := 0; i < 30; i++ {
		_, stop, _, err := Dirs([]string{t.TempDir()}, nil)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { stop(); stop(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("stop did not cancel and drain pending notification")
		}
	}
}

// The OS thread that issues a directory read can go on to run unrelated
// blocking synchronous I/O, as a supervised runtime's stdin reader does. Close
// from another goroutine must still cancel and drain while the directory
// changes. The deadline only reports a hang; the notifier has no bound.
func TestDirectoryNotificationCloseWhileArmingThreadBlocks(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	dir := t.TempDir()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	armed := make(chan func(), 1)
	blocked := make(chan struct{})
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		_, stop, _, err := Dirs([]string{dir}, nil)
		if err != nil {
			t.Error(err)
			stop = func() {}
		}
		armed <- stop
		var b [1]byte
		r.Read(b[:]) // synchronous ReadFile on the thread that issued the read
		close(blocked)
	}()
	stop := <-armed
	// Let the arming thread enter its read. Any directory change before Close
	// would complete the request and let the reporter rearm on its own thread.
	time.Sleep(20 * time.Millisecond)
	quit := make(chan struct{})
	writing := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(writing)
		for {
			select {
			case <-quit:
				return
			default:
			}
			_ = os.WriteFile(filepath.Join(dir, "changing.json"), []byte("{}"), 0600)
		}
	}()
	// Created last, Close runs first on the single P; changes race its cancel.
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Error("Close did not return while the arming thread was blocked in synchronous I/O")
	}
	close(quit)
	<-writing
	w.Write([]byte{1})
	w.Close()
	<-blocked
	<-stopped
}

func TestDirectoryNotificationCloseRacesChanges(t *testing.T) {
	for i := 0; i < 20; i++ {
		dir := t.TempDir()
		events, stop, _, err := Dirs([]string{dir}, nil)
		if err != nil {
			t.Fatal(err)
		}
		finished := make(chan error, 1)
		go func() {
			for j := 0; j < 20; j++ {
				if err := os.WriteFile(filepath.Join(dir, "changing.json"), []byte("{}"), 0600); err != nil {
					finished <- err
					return
				}
			}
			finished <- nil
		}()
		select {
		case <-events:
		case <-time.After(time.Second):
			t.Fatal("first edit not observed")
		}
		stopped := make(chan struct{})
		go func() { stop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(time.Second):
			t.Fatal("close/rearm race did not drain")
		}
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
		stop()
	}
}
