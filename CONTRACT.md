# abstraction.watch contract

Binds: `watch.thrift`

`watch.thrift` names `abstraction.watch/watcher@1`, a direct language
interface with no endpoint.

Watch owns observation: what a listener of a changing present is told
(`Poll`, `Push`, `Settle`), the one server-side waiter every service
`Observe` uses ([`Gate`](https://openabstractions.github.io/reference.html#concept-watch.waiter-gate)), the one asynchronous recheck pass (`Recheck`), and
the platform directory source (`Dirs`), and the Go `cursor` envelope codec.
Services keep their epochs, payload meaning, retention rules and authority;
the rules for waiting on them are here.
[README.md](README.md) is the door: what this module is, how to obtain it,
one example that runs. [HISTORY.md](HISTORY.md) records how this page changed.

## Reading this page

The Go `cursor` package owns envelope parsing, exact epoch/scope binding and
the existing confidential nonce+ciphertext framing with provider-supplied AEAD.
Providers keep their key and epoch provenance, associated data, payload
interpretation, snapshot tables and authorization, including rechecks after a
wake. A successful envelope binding grants no access. The codec keeps no state.
Existing provider cursor representations and refusal ordering are preserved.
Providers map malformed, foreign and expired envelopes to their published
outcomes; the codec supplies no service outcome vocabulary. Modelbridge's
`lend@1` continuation migration is specified in its MODELBRIDGE-L15 contract.

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY" and "OPTIONAL" in this page are to be
interpreted as described in RFC 2119 and RFC 8174, when, and only when, they
appear in all capitals, as shown here.

A rule id such as `WATCH-P1` is declared once, in bold brackets at the head
of the rule it names, on this page. Scenarios and tests cite it; a citation
that resolves to no rule here is a defect in one of the two. An id once
removed is never reused.

## What a listener receives

**[WATCH-P1]** The first [notice](https://openabstractions.github.io/reference.html#concept-notice) after attaching is the snapshot, whether or
not anything changed since before the listener attached — attaching late shows
a running job as running, never a history to replay.

**[WATCH-P2]** The same snapshot twice is not a change; what counts as visible
is the source's to say, and for a job record it is `JOB-N1` on
[the job page](https://github.com/openabstractions/abstraction-job).

**[WATCH-C1]** A listener that took nothing while several changes landed
receives one notice carrying the latest, and the source never waited for it.

**[WATCH-Q1]** While reads succeed, quiet is reported to a waiting listener
once the budget has passed with nothing visible, carrying the snapshot.

**[WATCH-Q2]** While reads succeed, it repeats each budget while the silence lasts.

**[WATCH-Q3]** A change ends the silence: the next one is measured from the
change.

A listener without a budget is never told quiet.

**[WATCH-U1]** A [polled source](https://openabstractions.github.io/reference.html#concept-watch.source) that cannot be read is told once per outage:
one notice with unavailable, carrying the last good snapshot and unread. No
quiet is told while reads fail. The first successful read after an outage is
told even when its snapshot is unchanged.

**[WATCH-X1]** After close a listener receives closed and nothing else; a
change made after close is not delivered to it.

**[WATCH-X2]** Two listeners on one source are independent: each has its own
snapshot, and taking a notice from one takes nothing from the other.

**The listener's own wait is the only timer.** Quiet is judged by a listener
that is waiting, after it has read, so a change the source already made is
always delivered ahead of a quiet that would postdate it. A single-threaded
driver cannot script a write that lands mid-wait, so this one is held by a
unit test in each language rather than a scenario:
`TestAPolledSourceIsReadBeforeSilenceIsJudged` in
[`go/watch_test.go`](go/watch_test.go), its namesake in
[`python/test_abstraction_watch.py`](python/test_abstraction_watch.py) and in
[`cpp/test/test_watch.cpp`](cpp/test/test_watch.cpp).

## Waiting on a service: [`Gate`](https://openabstractions.github.io/reference.html#concept-watch.waiter-gate)

A service `Observe` that waits MUST wait through one shared waiter per
source that keeps WATCH-W1 to W4 and T1, in every language a service is
written in. In Go that waiter is `watch.Gate`; the `config`, `asks`,
`logging`, `storage` and application-directory observations use it, and the waiters that
still loop on their own are listed under [Not built](#not-built). The service
supplies a [probe](https://openabstractions.github.io/reference.html#concept-watch.wait-probe): `Read` reads the present against the caller's position and
says whether there is something to answer; `Recheck` confirms the caller may
still be answered; `Deadline`, when set, names the next moment the present
changes by itself.

The waiter runs in the service's process, local or remote. A caller on
another machine waits through the same `Observe` call and reads the same
outcomes as a local one.

**[WATCH-W1]** A wait is at most 30 000 ms. A longer or negative wait is
refused before anything is read, and the service answers it as invalid. A
wait of zero reads once and takes no waiter slot.

**[WATCH-W2]** Waiters are bounded per gate. The waiter beyond the bound is
refused at once, never queued, and the service answers it as unavailable.

**[WATCH-W3]** The gate is armed before every read. An advance after a read,
including one landing before the waiter starts to wait, wakes it; no change
is lost between reading and waiting. After the wait expires the probe reads
once more, and with nothing to answer the outcome is unchanged.

**[WATCH-W4]** The recheck runs after every read and before anything is
returned. A recheck that refuses ends the wait with its refusal and no data,
so authority withdrawn during a 30 s wait is answered as refused.

**[WATCH-T1]** A source that can no longer observe ends with failed and a
cause, exactly once. Every waiter wakes and is told failed, and so is every
later wait; closed stays reserved for the consumer that closed it.

**[WATCH-H1]** A source MAY name its next known deadline, such as a grant's
expiry. The subscription (`PollDeadline`) or the wait (`Probe.Deadline`)
reads again at that deadline rather than at the next interval or the wait's
end, so an expiry is told without a periodic timer.

Each rule is held by a test in [`go/gate_test.go`](go/gate_test.go):
expiry and invalid wait (W1), the refused extra waiter (W2), the change
between read and wait (W3), the recheck after every wake (W4), failure once
(T1), and both deadline wakes (H1), together with cancel and a gap answered
on the wake that follows an epoch change.

## Sources and their mechanism

**[WATCH-S1]** A platform source (today `Dirs`) declares its
[Mechanism](https://openabstractions.github.io/reference.html#concept-watch.change-mechanism), which reports how it learns of
change on the running platform: `native` (the platform tells it), `polled`
(it is asked at an interval) or `none`. A platform without directory
notification says `none` rather than hiding a poll in a latency. A `Poll`
subscription is polled and a `Push` subscription is told by its owner by
construction, so they declare nothing. `DirsDeclared` reports the mechanism of `Dirs` on the
running platform to its Go callers; `DirsPlatformAPI` names the platform call
it uses. No service API carries the declaration yet ([Not built](#not-built)).
Held by `TestDirsDeclaredMatchesThePlatform` in
[`go/gate_test.go`](go/gate_test.go).

**[WATCH-D1]** `Dirs` is the one directory source per platform:
`ReadDirectoryChangesW` on Windows, `inotify` on Linux, `kqueue` on macOS and
the BSDs. It is armed before it returns, so a write made right after is
seen. A watched directory that is deleted, moved or unmounted ends the source
with the cause `ErrRootGone` (WATCH-T1); a source its owner stops ends with no
cause. Changes are coalesced: the source says that something moved, never
what, and its consumer rereads. Held by
[`go/dirs_end_test.go`](go/dirs_end_test.go) and
[`go/dirs_windows_test.go`](go/dirs_windows_test.go).

## Recheck passes

`watch.Recheck` is the one asynchronous recheck pass: a request returns at
once; requests arriving during a pass fold into exactly one more pass; each
pass runs under its owner's context and an optional per-pass deadline; `Stop`
refuses later requests, cancels the pass in flight and waits for it.
`NewCheckedRecheck` accepts a checked pass; `RequestAndWait` returns the
actual error of the pass covering its request. A request arriving
during a pass belongs to the next coalesced pass. Later requests do not delay
a completed join. A cancelled or expired pass is reported only after it can
no longer act; it is not successful settlement. This additive Go helper is
new unpublished 0.8 source; the published go/v0.3.0 has no Recheck API. Held by
`TestRecheckCoalescesAndStops` and `TestRecheckPassHasADeadline`.
Inference/resource rights-change composition joins checked requested passes;
Job and the Asks Book-pinned settler keep asynchronous requests.

Resource's yield `Book.Observe`, local-store inventory `ObserveContext` and
inference delta observations use `Gate`. Resource advances when its yield
requests change; inference shares a gate across admitted operations and
advances on deltas. Local-store inventory rereads its polled source through
the gate's deadline. Resource and local-store receivers recheck their binding
and admission after reads; inference rechecks operation visibility. Waiter
exhaustion uses each service's existing unavailable answer.

Runtime program-registration observations use the application directory's
shared gate. Registration edits and provider observations advance that gate;
the registry keeps its cursor/gap and page bounds and rechecks manage authority
after every read. Upstream inference APIs that supply no waiting observation
retain their protocol polling cadence; job-store sweeps retain their existing
notification and deadline schedule.

`watch.Follow` is the Go client follower over generated native page streams.
Its typed opener binds context and cursor; its typed position reader identifies
the cursor and gap. Gap restarts from an empty-cursor snapshot. A gap on that
fresh snapshot is a source error and ends the follower. Transport errors are
delivered once and end the invocation; reconnect belongs to the owner. Typed
adopters end their stream after delivering a refusal. Page delivery is
unbuffered. Close cancels the native wait and joins the worker, including blocked
delivery. This additive Go helper is new unpublished 0.8 source; published
Go 0.3 has no Follow API. Rights' actual Panel adapter uses it through generated
ObservePolicy pages. Its source is policy OnChange plus expiry through Gate;
the browser retains the genuine observation cursor and confirms expiry through
the existing ReadRule presentation. The Job example uses `FollowWork` on
operations@1 (JOB-N2), with the generated bounded page sequence, fresh-snapshot
restart on gap, and cancellation/join when its presenter ends. The FileStore
provider reports its polled `job.Watch` mechanism explicitly.

## Scenarios

The notice rules (P, C, Q, X) are held by the `notice-*` scenarios in
`abstraction-download/testdata/scenarios/` (`../abstraction-download/testdata/scenarios`),
which the download driver runs over a job store.

The other rules are held by unit tests:

- WATCH-U1 in each language that carries `Poll`:
  `TestAFailingPolledSourceIsUnavailableAndNeverQuiet` in
  [`go/watch_test.go`](go/watch_test.go),
  `test_a_failing_polled_source_is_unavailable_and_never_quiet` in
  [`python/test_abstraction_watch.py`](python/test_abstraction_watch.py) and
  `a_failing_polled_source_is_unavailable_and_never_quiet` in
  [`cpp/test/test_watch.cpp`](cpp/test/test_watch.cpp).
- WATCH-W1 to W4, T1 and H1 in [`go/gate_test.go`](go/gate_test.go). `Gate`
  and `PollDeadline` exist in Go only ([Divergences](#divergences)), so one
  implementation holds them.
- WATCH-D1 and S1 in the `Dirs` tests named above. They depend on the
  running platform's notification call.

The listener tests above read the shared [U1 outage fixture](go/testdata/README.md):
two outage/recovery cycles with a real-time observation deadline, one unavailable
notice per outage, no quiet during failure, and same-stamp recovery. H1 joins
the shared fixture when deadline sources exist in Python and C++.

## Divergences

- `Gate`, `Recheck`, `Follow`, `Dirs`, `PollDeadline` and `DirsDeclared` exist in Go
  only. Python and C++ carry the listener (`Poll`, `Push`, `Settle`); their
  services wait in Go today.

## Not built

- **IPC-A7 remaining adoption.** The generated native observation construct
  is received against `ConfigObserver` in five languages and Rights' actual Go owner.
  Other cursor shapes and final-version Job observation remain pending.
- **Decided waiters in serve.** `runtime_asks.go` and
  `runtime_self_registered.go` wait on `Book.Decided` inside one process,
  answering no `Observe` call, and the deprecated asks service keeps its
  own wait until the asks Go module's next major version removes it.
- **The source's mechanism over a service API (WATCH-S1).** The `config`
  module's `Observe` does not report whether its source is native, polled or none.
