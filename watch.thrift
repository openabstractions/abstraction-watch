namespace * abstraction.watch.api

// Shared observation metadata; generic payload and subscription lifetime stay native.
encoding json {
 escape="minimal"
 indent="2"
 map_keys="utf8-bytes"
 numbers="integer-decimal"
 opaque="verbatim"
 terminator="newline"
 duplicate_keys="refuse"
 depth_limit="64"
}
refusal {
 1: malformed(stage="grammar")
 2: bad_string(stage="grammar")
 3: number_spelling(stage="grammar")
 4: wrong_type(stage="grammar")
 5: depth_exceeded(stage="grammar")
 6: duplicate_key(stage="grammar")
 7: duplicate_field(stage="structure")
 8: unknown_field(stage="structure")
 9: missing_field(stage="structure")
 10: trailing_bytes(stage="document")
}
struct NoticeMetadata {
 1: required bool quiet
 2: required i64 silence_ns
}(document="true",unknown_fields="refuse",doc="Quiet means no visible change for at least the configured budget. Silence is elapsed monotonic observer duration in nanoseconds. This is metadata of Notice<T>; arbitrary native T is not serialized or replaced by this descriptor.")
struct WatchOptions {
 1: required i64 every_ns
 2: required i64 budget_ns
}(unknown_fields="refuse",doc="Polling interval and quiet budget in nanoseconds. Existing native provider normalization applies. Clock origins are never transferred.")
struct Stamp {
 1: required string value
}(unknown_fields="refuse",doc="Opaque change token supplied beside a native payload by Source<T> or Post. A changed stamp denotes a visible change; it is not an authority or global timestamp.")
const list<string> lifecycle_operations = ["Poll", "Push", "Post", "Current", "Next", "Close", "Settle"]
const list<string> failure_names = ["closed"]
// Binding obligation: Notice<T>.Now, Source<T> callback, Subscription<T>, native
// context cancellation and Settle callback retain their existing typed lifetime.
// This schema generates vocabulary only. It does not claim a generic interface
// or streaming IPC; each language binding supplies those native types explicitly.


