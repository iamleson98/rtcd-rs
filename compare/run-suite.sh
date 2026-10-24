#!/bin/bash
# Builds one of the two servers and runs the whole suite against it, writing text results that
# report.py compares:
#
#   compare/run-suite.sh go|rust <output dir>
#
# unit.txt (the project's own tests), interop.txt (Go service client + pion scenarios), browser.txt
# (the LMS call client in headless Chrome), load3.txt and load10.txt (3 and 10 classes of 30).
# A failing step is recorded, not fatal: the report decides.
set -uo pipefail
SERVER=${1:?go or rust}
mkdir -p "${2:?output dir}"
OUT=$(cd "$2" && pwd)
ROOT=$(cd "$(dirname "$0")" && pwd)
# The Rust service is the repository this directory sits in.
REPO=$(cd "$ROOT/.." && pwd)
H="$ROOT/harness/interop"
step() { echo "::group::$SERVER: $1"; }
done_() { echo "::endgroup::"; }

step "build"
case $SERVER in
  go) (cd "$ROOT/rtcd-go" && go build -o "$OUT/rtcd" ./cmd/rtcd) ;;
  rust) (cd "$REPO" && cargo build --release -p rtcd && cp target/release/rtcd "$OUT/rtcd") ;;
  *) echo "unknown server $SERVER"; exit 2 ;;
esac || { echo "build failed" > "$OUT/build-failed"; exit 1; }
done_

step "unit tests"
case $SERVER in
  go) (cd "$ROOT/rtcd-go" && go test ./... 2>&1) > "$OUT/unit.txt" ;;
  rust) (cd "$REPO" && cargo test --workspace --no-fail-fast 2>&1) > "$OUT/unit.txt" ;;
esac
tail -5 "$OUT/unit.txt"
# What failed, where the run's annotations can show it (the full output stays in the artifact).
if grep -qE "FAILED|^FAIL|panicked" "$OUT/unit.txt"; then
  echo "::warning title=$SERVER unit test failures::$(grep -E "^test .* FAILED|^--- FAIL|^FAIL|panicked at" -A6 "$OUT/unit.txt" | head -60 | sed -e 's/%/%25/g' | awk '{printf "%s%%0A", $0}')"
fi
done_

step "harness bundle"
bun "$ROOT/harness/browser/build.ts" "$OUT/harness.js" || echo "bundle failed" > "$OUT/bundle-failed"
done_

NOSANDBOX=""
[ "$(uname)" = Linux ] && NOSANDBOX="-no-sandbox"

step "interop scenarios"
"$H/bench.sh" "$OUT/rtcd" -scenario all > "$OUT/interop.txt" 2>&1
grep -E "^==|PASS|FAIL|passed|failed" "$OUT/interop.txt"
done_

step "browser"
"$H/bench.sh" "$OUT/rtcd" -browser "$OUT/harness.js" $NOSANDBOX > "$OUT/browser.txt" 2>&1
grep -E "PASS|FAIL|INFO|passed|failed" "$OUT/browser.txt"
done_

step "load, 3 classes"
"$H/bench.sh" "$OUT/rtcd" -calls 3 -seconds 20 > "$OUT/load3.txt" 2>&1
grep -E "joined|join time|voice|camera|server" "$OUT/load3.txt"
done_

step "load, 10 classes"
if [ "$(uname)" = Linux ] && [ "${PROFILE:-}" = 1 ] && command -v perf >/dev/null; then
  export BENCH_PERF="$OUT/perf.data"
fi
"$H/bench.sh" "$OUT/rtcd" -calls 10 -seconds 30 > "$OUT/load10.txt" 2>&1
if [ -f "$OUT/perf.data" ]; then
  {
    echo "== by library"
    sudo perf report -i "$OUT/perf.data" --stdio --no-children --sort dso 2>/dev/null | grep -v '^#' | grep -v '^$' | head -8
    echo "== top symbols"
    sudo perf report -i "$OUT/perf.data" --stdio --no-children --sort dso,symbol 2>/dev/null | grep -v '^#' | grep -v '^$' | head -45
  } | tr -s ' ' | sed -e 's/<rtc[a-z_:]*::\([A-Za-z]*\) as sansio[^>]*>/<\1 as Protocol>/' | cut -c1-150 > "$OUT/profile.txt"
  # perf ran as root: its files must not stop the results from being uploaded.
  sudo rm -f "$OUT"/perf.data*
  echo "::notice title=$SERVER CPU profile::$(sed -e 's/%/%25/g' "$OUT/profile.txt" | awk '{printf "%s%%0A", $0}')"
  { echo "### $SERVER CPU profile"; echo '```'; cat "$OUT/profile.txt"; echo '```'; } >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
fi
grep -E "joined|join time|voice|camera|server" "$OUT/load10.txt"
done_
if [ "$SERVER" = rust ]; then
  for n in 1 2 4; do
    step "load, 10 classes, $n shard(s)"
    RTCD_RTC_SHARDS=$n "$H/bench.sh" "$OUT/rtcd" -calls 10 -seconds 30 > "$OUT/load10-s$n.txt" 2>&1
    grep -E "voice|camera|server" "$OUT/load10-s$n.txt"
    done_
  done
fi
rm -f "$OUT/rtcd"
