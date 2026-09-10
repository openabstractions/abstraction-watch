# abstraction-watch, in Python

Be told what is true now, and be told when nothing has changed for a while.
A `Notice` carries the present; `quiet` says nothing visible has moved for at
least the budget, and `silence` says for how long. One module, standard library
only, importing nothing of ours.

Quiet is the half that is normally missing. A progress bar that stops moving and
a transfer nobody is performing look identical to a listener that is only told
about changes.

This page is the Python package. The contract, the Go and C++ implementations,
what is measured and what is `UNPROVEN` are on
[the repository](https://github.com/openabstractions/abstraction-watch).

## Install

Not on PyPI, and the name on PyPI is not ours.

    git clone https://github.com/openabstractions/abstraction-watch
    pip install ./abstraction-watch/python

Python 3.9 or later. `abstraction_watch.py` is one file with no imports of ours,
so copying it into a `_vendor/` directory of your own is an equally complete
installation.

## An example that runs

A source that says when it moved, and a listener that hears both the change and
the quiet after it:

```python
import abstraction_watch as watch

progress = watch.push({"done": 0}, stamp="0", budget=0.5)
progress.post({"done": 1 << 20}, stamp="1")

notice = progress.next()
print("now:", notice.now)

notice = progress.next()
print("quiet:", notice.quiet, "after", round(notice.silence, 1), "s of nothing")
progress.close()
```

`budget=0.5` is the quiet budget and the only timer here: the listener's own wait
decides when silence counts, so a change the source already made is always
delivered ahead of a quiet that would postdate it.

## What an application calls

| call | what it does |
|---|---|
| `push(first, stamp, budget)` | a subscription over a source that reports its own changes through `post` |
| `poll(read, every, budget)` | a subscription over a source that has to be asked. `read` returns `(value, stamp)`; `every` is the interval between asks while a listener waits |
| `Subscription.next(timeout=None)` | the next `Notice`: a change, or quiet once the budget passed with none |
| `Subscription.current()` | what is true now, without waiting |
| `Subscription.post(value, stamp)` | tell a `push` subscription the source moved |
| `Subscription.close()` | end it. A waiting `next` raises `Closed` |
| `settle(events, period, settled)` | call `settled` once a queue of platform notifications has been still for `period`, because one edit produces several of them |

A subscription is iterable: `for notice in subscription: ...`, ending on `close()`.
`stamp` is what decides whether a value is a change; two reads with the same stamp
are one.

`abstraction_job.watch(store, kind, budget)` is this primitive with a job store as
the source, which is what a download progress view binds to.

## What may break

- **No conformance verdict, in any language.** No scenario in the suite cites
  this layer yet — [what is proven and what is not](https://openabstractions.org/coverage.html).
  Anything that depends on this layer inherits that.
- **Not on any package index**, and no release carries an API stability promise.
  Pin a commit you have read.
- **`poll` asks; it does not subscribe to the platform.** There is no inotify,
  no `ReadDirectoryChangesW` and no FSEvents under it. `settle` is the seam
  where a caller supplies those.
- Every published transcript was produced on Windows or Linux. macOS is
  `UNPROVEN` throughout.

Apache-2.0. See [LICENSE](LICENSE).
