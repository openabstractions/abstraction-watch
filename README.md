# abstraction-watch

*For provider implementers. An application uses a capability service's own
cursor/gap observation contract; one reading job progress calls the job
service's `ObserveWork`.*

A debounce and settle helper: it tells a consumer what is true now and when that visible state has stopped
changing for a chosen time. Providers use this to recognize a stalled transfer,
a quiet lease or a burst of filesystem changes without each writing its own
polling loop. `Poll`, `Push` and `Settle` cover common source styles.

**What's new here:** Settling is an event: a consumer gets the current state,
then one [notice](https://openabstractions.github.io/reference.html#concept-notice) once a burst of changes has gone quiet.

A notice carries what is true now and whether it has stopped changing. It is a
snapshot rather than an event history. Capability services with generated
cursor/gap observation contracts use those service APIs at the application
boundary.

Every platform can say a thing happened. None of them says a thing has *stopped*
happening — a transfer whose owner died, a service that answered an hour ago, a
lease nobody renewed — so every caller that needs the absence writes a loop that
wakes up to check. Three of ours did in one week without noticing each other:
`Deliver` ended on a record going quiet under a tick of its own, the keeper
counted beats against a budget of its own, and the agent ledger's "still
running" row was the same gap a third time. This layer is that loop, written
once, with the absence as a first-class fact. The example is in
[Example](#example).

The conventions every OA contract carries are stated once in [CONVENTIONS.md](https://github.com/openabstractions/abstractions/blob/main/idl/CONVENTIONS.md); a rule here that applies one cites its `CONV-` id.

## Words

```
now        what is true, as of the last read
quiet      nothing visible has changed for at least the budget
silence    for how long, by this observer's clock
```

| word | meaning |
|---|---|
| **notice** | the snapshot, not an event and not a history; a listener asks *what now?* and is answered with it |
| **budget** | how long nothing visible may change before the listener is told quiet |
| **visible** | what counts as a change is the source's to say; for a job record it is `JOB-N1` on [the job page](https://github.com/openabstractions/abstraction-job) |
| **source** | polled (`Poll`: [something that must be asked](https://openabstractions.github.io/reference.html#concept-watch.source)) or pushed (`Push`: something that says when it moved) |
| **unavailable** | the last read of a polled source failed; the notice carries the last good snapshot |
| **unread** | how long since the last successful read, by this observer's clock |
| **settle** | the same fact told from the source's side: run once the signals have stopped for the budget |

Rules are tagged `WATCH-*` on [CONTRACT.md](CONTRACT.md); the `notice-*` scenarios in
[abstraction-download/testdata/scenarios](https://github.com/openabstractions/abstraction-download/tree/main/testdata/scenarios)
cite them, and the behaviour harness reports which have no scenario.

## Obtain

Install the runtime first: https://openabstractions.org/adopt.html

- **Go.** `go get github.com/openabstractions/abstraction-watch/go`.
  [Releases, newest first](https://github.com/openabstractions/abstraction-watch/tags);
  pin the exact tag you tested against.
- **Python.** Not on any index —
  [what to install, import and call](python/README.md).
- **C++.** Header only: `cpp/include` on the include path. No tagged release.

Whether to adopt this at all, what it costs and what is not proven:
[Adopting](CONTRIBUTING.md#adopting).

## Example

**Go**

```go
sub := job.WatchQuiet(store, download.Kind, 2*ttl)
defer sub.Close()
for {
    n, err := sub.Next(ctx)          // a change, or quiet once 2*ttl passed with none
    if err != nil { return err }     // ctx, or watch.ErrClosed
    if n.Quiet && nobodyHolds(n.Records) { return errAbandoned }
}
```

The generic primitive under that is `watch.Poll(read, every, budget)` for a
source that must be asked and `watch.Push(first, stamp, budget)` for one that
says when it moved; `job` supplies the source and what "visible" means.

## Quiet from the source's side

`watch.Settle(ctx, in, budget, settled)` is the same fact told the other way:
`settled` runs once the signals on `in` have stopped for the budget, and never
while they are still arriving. A platform notification is the case it exists
for — a text editor saving a file produces several events and a subscriber
wants one, and `config` uses it to turn `ReadDirectoryChangesW`, `inotify` and
`kqueue` into a single answer.

`Notice.Quiet` cannot serve here. It repeats every budget while the silence
lasts (`WATCH-Q2`), which is what a listener asking *has this stopped?* needs and
exactly wrong for a debounce, which must fire once and then cost nothing. `Settle`
waits on the channel while nothing is happening, so an idle source wakes nobody.
**Go only so far**; Python and C++ have no caller for it yet.

## What a listener receives

The rules a listener, a waiting service and a directory source follow are
on [CONTRACT.md](CONTRACT.md), each with its `WATCH-*` id: a listener's
notices (P1, P2, C1, Q1 to Q3, U1, X1, X2), the shared service waiter (W1 to
W4, T1, H1) and the sources (S1, D1).

## Today

- **Go**: `Poll`, `PollDeadline`, `Push`, `Settle`, [`Gate`](https://openabstractions.github.io/reference.html#concept-watch.waiter-gate), `Recheck` and
  `Dirs`; `job.WatchQuiet` and download's `Deliver` sit on it, and the config,
  storage, logging, asks and runtime directory and registry `Observe` wait on
  `Gate`.
- **Python**: `poll`, `push`, `settle`.
- **C++**: `Subscription::poll`, `Subscription::push`.

The file binding has nothing to push, so it is asked: on entry to `next`, and
then every `every` while a listener waits — 750 ms for a job store, or the
budget when that is shorter. A change therefore lands within one `every`, and
quiet within one scheduler tick of the budget. The numbers are
`notice_latency_p50_ms`, `notice_latency_p95_ms` and `quiet_overshoot_p95_ms`
in the gate's series, which is not published yet, produced by
`measure/measure.sh`.

What may break:

- A source that cannot be read keeps the last good snapshot rather than
  becoming empty, so a store that blinks does not empty a window.
- Download's `Deliver` wakes on the unavailable notice and loads its job
  directly. A store that cannot be listed is never judged abandoned; when the
  load fails too, `Deliver` returns that error at once.
- `Next` and `Changes()` (Go) are two spellings of one stream; drive a
  subscription with one of them.
- A budget shorter than the store's poll interval shortens the interval to
  match, so a store on a share is asked as often as the caller asked to be told.

## Conformance

The `notice-*` scenarios in
[abstraction-download/testdata/scenarios](https://github.com/openabstractions/abstraction-download/tree/main/testdata/scenarios)
are replayed by the Go implementation and judged by
the internal `behaviour-conformance.sh` maintainer check —
[`BEHAVIOUR1.txt`](https://github.com/openabstractions/abstractions/blob/main/docs/results/BEHAVIOUR1.txt).
The one rule a scenario cannot script is held by the named unit test in each
language.

## Where it sits

Below: nothing of ours. Above:
[abstraction-job](https://github.com/openabstractions/abstraction-job)
supplies the source and what visible means,
[abstraction-download](https://github.com/openabstractions/abstraction-download)
waits for delivery on it, and
[abstraction-rights](https://github.com/openabstractions/abstraction-rights)
composes on it.

One layer of [openabstractions](https://github.com/openabstractions/abstractions).
Every layer names one thing local tools rebuild on their own; the name means the
same in each language that implements it, and the conformance scenarios are what
hold an implementation to it.

## Requirements

Go 1.25 or newer; the standard library and `golang.org/x/sys` for the directory source. Python 3.9 or newer, standard library
only. C++17: header-only, so `cpp/include` on the include path is enough, and
`cpp/CMakeLists.txt` installs a `find_package` package for the layers above it:

    find_package(abstraction_watch 0.8 CONFIG REQUIRED)
    target_link_libraries(your_target PRIVATE abstraction::watch)

## Status

**In development.** No standalone conformance scenario or stability promise.
Pin a reviewed entry from
[the tag list](https://github.com/openabstractions/abstraction-watch/tags).

## Licence

Apache-2.0. See [LICENSE](https://github.com/openabstractions/abstraction-watch/blob/main/LICENSE).

<!-- BEGIN GENERATED PERMISSION DEFINITIONS -->

## Permission definitions

Generated from the existing compiled catalogue. [RIGHTS.json](RIGHTS.json) contains the complete action/profile records, disclosure, lifecycle, first-use behavior, selector support and finite bundles.

This projection has no named action definition for Watch. Debounce and settle snapshots remain provider utilities; cursor/gap observation and admission belong to each capability service.

<!-- END GENERATED PERMISSION DEFINITIONS -->
