#!/bin/sh
# What waiting on a job store costs, measured rather than described.
#
# Runs the instrument in job/go/watch_measure_test.go against a real file store
# on this machine and records: how long after a write a waiting listener is
# told (the file binding is asked every 750 ms, so this is the poll interval's
# distribution plus the read), how far past its budget a quiet notice lands, and
# what one read of the store costs. Numbers go to research/gate/series.tsv;
# RESULTS.txt keeps the raw run and names the machine class, never the value
# in prose.
set -eu

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$(dirname "$0")/RESULTS.txt"
SERIES="$ROOT/research/gate/series.tsv"
DATE="$(date +%Y-%m-%d)"
COMMIT="$(git -C "$ROOT" rev-parse --short HEAD)"

raw="$(cd "$ROOT/job/go" && WATCH_MEASURE=1 go test -count=1 -run TestMeasureWatch . -v 2>&1 | grep -E '^(records|[a-z_]+_(p50|p95)_ms)[[:space:]]')"

{
    echo "watch: what waiting on a file-bound job store costs"
    echo "date $DATE  commit $COMMIT  host $(uname -s) $(uname -m)  go $(go version | cut -d' ' -f3)"
    echo "instrument job/go/watch_measure_test.go (WATCH_MEASURE=1); poll interval 750 ms; quiet budget 100 ms; store in the OS temp directory"
    echo
    echo "$raw"
    echo
    echo "verified for: the file binding on a local NTFS temp directory, one writer, one listener."
    echo "NOT examined: a store on SMB, a push binding, more than one listener, Python or C++ listeners."
} > "$OUT"

echo "$raw" | while IFS="$(printf '\t')" read -r metric value; do
    case "$metric" in records) continue ;; esac
    printf '%s\t%s\t%s\t%s\t%s\n' "$DATE" "$COMMIT" "$metric" "$value" "research/watch/measure.sh" >> "$SERIES"
done
cat "$OUT"
