# abstraction-watch

**In development.** No conformance scenario cites this layer on its own, and the
API carries no stability promise. Tags exist and no version number is typed on
this page: [the tag list](https://github.com/openabstractions/abstraction-watch/tags)
is the answer to "which release", because a tag is the only thing that cannot
drift.

A notice is the snapshot — what is true now, and whether it has stopped
changing — so a listener that needs the absence of change is told it, instead
of writing a loop that wakes up to check.

## The problem

Every platform can say a thing happened. None of them says a thing has *stopped*
happening — a transfer whose owner died, a service that answered an hour ago, a
lease nobody renewed — so every caller that needs the absence writes a loop that
wakes up to check. Three of ours did in one week without noticing each other:
`Deliver` ended on a record going quiet under a tick of its own, the keeper
counted beats against a budget of its own, and the agent ledger's "still
running" row was the same gap a third time. This layer is that loop, written
once, with the absence as a first-class fact.

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
| **source** | polled (`Poll`: something that must be asked) or pushed (`Push`: something that says when it moved) |
| **settle** | the same fact told from the source's side: run once the signals have stopped for the budget |

Rules on this page are tagged `WATCH-*`; the `notice-*` scenarios in
[abstraction-download/testdata/scenarios](https://github.com/openabstractions/abstraction-download/tree/main/testdata/scenarios)
cite them, and the behaviour harness reports which have no scenario.

## Obtain

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

**Python** — `for n in watch(store, KIND, budget=60): ...`, ending on `close()`.

**C++** — `auto sub = abstraction::job::watch(store, kKind, 60s); while (auto n = sub.next()) ...`

The generic primitive under those is `watch.Poll(read, every, budget)` for a
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
lasts (`WATCH-Q2` below), which is what a listener asking *has this stopped?* needs and
exactly wrong for a debounce, which must fire once and then cost nothing. `Settle`
waits on the channel while nothing is happening, so an idle source wakes nobody.
**Go only so far**; Python and C++ have no caller for it yet.

## What a listener receives, and a scenario for each

The first notice after attaching is the snapshot, whether or not anything
changed since before the listener attached — attaching late shows a running
job as running, never a history to replay [WATCH-P1]. The same snapshot twice is
not a change [WATCH-P2]; what counts as visible is the source's to say, and for a
job record it is `JOB-N1` on [the job page](https://github.com/openabstractions/abstraction-job).

A listener that took nothing while several changes landed receives one notice
carrying the latest, and the source never waited for it [WATCH-C1].

Quiet is reported to a waiting listener once the budget has passed with nothing
visible, carrying the snapshot [WATCH-Q1]. It repeats each budget while the silence
lasts [WATCH-Q2], and a change ends the silence: the next one is measured from the
change [WATCH-Q3]. A listener without a budget is never told quiet.

After close a listener receives closed and nothing else; a change made after
close is not delivered to it [WATCH-X1]. Two listeners on one source are
independent: each has its own snapshot, and taking a notice from one takes
nothing from the other [WATCH-X2].

**The listener's own wait is the only timer.** Quiet is judged by a listener
that is waiting, after it has read, so a change the source already made is
always delivered ahead of a quiet that would postdate it. A single-threaded
driver cannot script a write that lands mid-wait, so this one is held by a
unit test in each language rather than a scenario:
`TestAPolledSourceIsReadBeforeSilenceIsJudged` in
[`go/watch_test.go`](go/watch_test.go), its namesake in
[`python/test_abstraction_watch.py`](python/test_abstraction_watch.py) and in
[`cpp/test/test_watch.cpp`](cpp/test/test_watch.cpp).

## Today

- **Go**: `Poll`, `Push`, `Settle`; `job.WatchQuiet` and download's `Deliver`
  sit on it.
- **Python**: `watch(store, kind, budget)`.
- **C++**: `abstraction::job::watch`.

The file binding has nothing to push, so it is asked: on entry to `next`, and
then every `every` while a listener waits — 750 ms for a job store, or the
budget when that is shorter. A change therefore lands within one `every`, and
quiet within one scheduler tick of the budget. The numbers are
`notice_latency_p50_ms`, `notice_latency_p95_ms` and `quiet_overshoot_p95_ms`
in the gate's series, which is not published yet, produced by
[`measure/measure.sh`](measure/measure.sh).

What may break:

- A source that cannot be read keeps the last good snapshot rather than
  becoming empty, so a store that blinks does not empty a window.
- `Next` and `Changes()` (Go) are two spellings of one stream; drive a
  subscription with one of them.
- A budget shorter than the store's poll interval shortens the interval to
  match, so a store on a share is asked as often as the caller asked to be told.

## Conformance

The `notice-*` scenarios in
[abstraction-download/testdata/scenarios](https://github.com/openabstractions/abstraction-download/tree/main/testdata/scenarios)
are replayed by all three languages and diffed by
[`behaviour-conformance.sh`](https://github.com/openabstractions/abstractions/blob/main/scripts/behaviour-conformance.sh) —
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

Go 1.22 or newer, standard library only. Python 3.9 or newer, standard library
only. C++17: header-only, so `cpp/include` on the include path is enough, and
`cpp/CMakeLists.txt` installs a `find_package` package for the layers above it:

    find_package(abstraction_watch 0.1 CONFIG REQUIRED)
    target_link_libraries(your_target PRIVATE abstraction::watch)

## Licence

Apache-2.0. See [LICENSE](https://github.com/openabstractions/abstraction-watch/blob/main/LICENSE).
