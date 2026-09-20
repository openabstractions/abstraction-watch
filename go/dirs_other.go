//go:build !windows && !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly

package watch

import "errors"

const DirsPlatformAPI = ""

// This platform is asked rather than telling. Said out loud rather than hidden
// in a latency: How reports it, and a window can say so.
func Dirs(_, _ []string) (<-chan struct{}, func(), func() error, error) {
	return nil, nil, nil, errors.New("this platform has no directory notification")
}
