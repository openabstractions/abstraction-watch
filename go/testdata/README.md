# Polled-source outage fixture

`poll-outage.txt` holds five whitespace-separated integers: quiet budget in
milliseconds, polling interval in milliseconds, outage observation deadline in
milliseconds, number of outage/recovery cycles, and expected unavailable
notices per outage.

This location is inside the published Go submodule. The Go, Python and C++
listener tests read this same statement. Each starts
with present `a`, stamp `1`, turns the outage on, and listens until the real-time
deadline. Every notice retains `a`, reports unavailable and carries no quiet.
The count equals the fixture's expectation. Turning the outage off delivers
the same present and stamp with unavailable cleared and no quiet. The next
cycle proves that a recovered listener reports a fresh outage.

WATCH-H1 remains covered by Go's `PollDeadline` tests. Python and C++ have no
deadline-source API yet.
