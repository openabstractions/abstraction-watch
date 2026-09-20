# History: abstraction.watch

Linked from [CONTRACT.md](CONTRACT.md)'s opening.

## 2026-10-20: watch owns the service waiter (W1-07)

The WATCH-* rules moved from the README to CONTRACT.md, which declares
each once. Watch gained the server-side waiter `Gate` (WATCH-W1 to W4, T1,
H1), the asynchronous `Recheck` pass and the platform directory source
`Dirs` (WATCH-D1, S1), which moved here from abstraction-config's notify
files. `PollDeadline` reads a source again at the deadline it names.
`DirsMechanism` was renamed `DirsPlatformAPI`, so mechanism means only
native, polled or none. A deadline already passed is ignored rather than
re-read every millisecond, and a source that fails after naming a deadline
is reread at its interval. The generated observe construct (IPC-A7) and
three service waiters remain under Not built.

The Gate requirement is stated for every language, with `watch.Gate` as the
Go waiter, and says the waiter runs where the service runs, local or remote.
WATCH-S1 binds platform sources (today `Dirs`). Scenarios names the unit
tests that hold U1 in Go, Python and C++ and links the notice scenarios
where they live.

Adopters: watch now requires `golang.org/x/sys` (BSD-3-Clause) and Go
1.25.0, the floor its go directive moved to from 1.22. A Poll- or Push-only
consumer takes both.
