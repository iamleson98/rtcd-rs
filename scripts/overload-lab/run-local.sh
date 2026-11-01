#!/usr/bin/env bash
# The overload lab without containers: a release rtcd and lms-sim on this machine, loopback between
# them. There is no kernel send path to throttle (no qdisc on loopback), so what it loads is the
# shards' threads: the lag, and what a shard that falls behind keeps. On macOS every datagram arrives
# at one shard and is passed on to the one that owns the session, which is the path under test.
#
#   scripts/overload-lab/run-local.sh RTCD_BINARY LMS_SIM_BINARY LABEL [RTCD_...=...]
#
# Settings by environment: SHARDS (2), CLASSES, STUDENTS, CAMERAS, WATCH, PER_SECOND, HOLD, SAMPLE.
set -euo pipefail
cd "$(dirname "$0")"
RTCD=${1:?rtcd binary}; SIM=${2:?lms-sim binary}; LABEL=${3:?label}; shift 3
SHARDS=${SHARDS:-2}; CLASSES=${CLASSES:-4}; STUDENTS=${STUDENTS:-60}; CAMERAS=${CAMERAS:-8}
WATCH=${WATCH:-3}; PER_SECOND=${PER_SECOND:-10}; HOLD=${HOLD:-120}; SAMPLE=${SAMPLE:-5}
ASSETS=${ASSETS:-${TMPDIR:-/tmp}/lms-sim-assets}
ulimit -n 10240 2>/dev/null || true
mkdir -p out
dir=$(mktemp -d)
cat >"$dir/config.toml" <<TOML
[api.http]
listen_address = "127.0.0.1:8045"
[api.security]
allow_self_registration = true
[rtc]
ice_port_udp = 8443
ice_port_tcp = 8443
ice_host_override = "127.0.0.1"
ice_servers = []
[store]
data_source = "$dir/db"
[logger]
enable_console = true
console_level = "WARN"
console_json = false
enable_file = false
TOML
env RTCD_RTC_SHARDS="$SHARDS" "$@" "$RTCD" -config "$dir/config.toml" >"out/$LABEL.rtcd.log" 2>&1 &
rtcd_pid=$!
trap 'kill $rtcd_pid ${gen_pid:-} ${sample_pid:-} 2>/dev/null || true; rm -rf "$dir"' EXIT
until curl -sf localhost:8045/version >/dev/null 2>&1; do sleep 0.5; done
RTCD_PID=$rtcd_pid python3 sample.py "$SAMPLE" 100000 >"out/$LABEL.jsonl" &
sample_pid=$!
LMS_SIM_ASSETS="$ASSETS" "$SIM" load --http http://127.0.0.1:8045 --media 127.0.0.1:8443 \
  --classes "$CLASSES" --students "$STUDENTS" --cameras "$CAMERAS" --watch "$WATCH" \
  --per-second "$PER_SECOND" --seconds "$HOLD" >"out/$LABEL.gen.log" 2>&1 &
gen_pid=$!
echo "run $LABEL: $((CLASSES * (STUDENTS + 1))) people on $SHARDS shards, joined at $PER_SECOND/s, then $HOLD s"
while kill -0 $gen_pid 2>/dev/null && kill -0 $rtcd_pid 2>/dev/null; do sleep 2; done
kill -0 $rtcd_pid 2>/dev/null || echo "the node is gone"
