//go:build windows || linux || darwin || freebsd || netbsd || openbsd || dragonfly

package watch

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A native source whose watched directory is deleted closes its channel with a
// cause within one settle period instead of going quiet.
func TestNativeSourceEndsWhenDirectoryIsDeleted(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "watched")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	events, stop, cause, err := Dirs([]string{dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-events:
			if ok {
				continue
			}
			if !errors.Is(cause(), ErrRootGone) {
				t.Fatalf("cause = %v", cause())
			}
			return
		case <-deadline:
			t.Fatal("deleted directory left the notification open")
		}
	}
}

// RemoveAll unlinks a watched file before its directory; the source
// must still end with a cause.
func TestNativeSourceEndsWhenDirectoryWithFileIsDeleted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "watched", "config.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	events, stop, cause, err := Dirs([]string{filepath.Dir(path)}, []string{path})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if err := os.RemoveAll(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-events:
			if ok {
				continue
			}
			if !errors.Is(cause(), ErrRootGone) {
				t.Fatalf("cause = %v", cause())
			}
			return
		case <-deadline:
			t.Fatal("deleted directory with a file left the notification open")
		}
	}
}

func TestNativeSourceStoppedHasNoCause(t *testing.T) {
	events, stop, cause, err := Dirs([]string{t.TempDir()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	stop()
	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("event after stop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stop did not close the notification")
	}
	if cause() != nil {
		t.Fatalf("stop reported cause %v", cause())
	}
}

// A watched directory renamed away ends the source with ErrRootGone, though
// nothing changes inside it (WATCH-D1).
func TestNativeSourceEndsWhenDirectoryIsMoved(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "watched")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	events, stop, cause, err := Dirs([]string{dir}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if err := os.Rename(dir, filepath.Join(base, "moved")); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-events:
			if ok {
				continue
			}
			if !errors.Is(cause(), ErrRootGone) {
				t.Fatalf("cause = %v", cause())
			}
			return
		case <-deadline:
			t.Fatal("moved directory left the notification open")
		}
	}
}

// A file moved out of a watched directory is a change there.
func TestNativeSourceSeesFileMovedOut(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "watched")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	events, stop, _, err := Dirs([]string{dir}, []string{path})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if err := os.Rename(path, filepath.Join(base, "config.bak")); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-events:
		if !ok {
			t.Fatal("source ended instead of reporting the move")
		}
	case <-time.After(time.Second):
		t.Fatal("file moved out was not reported")
	}
}
