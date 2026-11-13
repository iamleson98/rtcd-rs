#!/usr/bin/env bash
# One lesson soak against one SFU: the SFU in a container limited to 2 CPUs, the lesson in two client
# containers (a clean network and an impaired one), a headless Chromium in each group, and a sampler
# that reads the SFU's process from outside. In the impaired group every participant is shaped on its
# own by netem.sh (a downlink and an uplink cap, delay and loss for each one's UDP media path: the
# synthetic students by the block of ports each binds to, Chromium by a network namespace of its
# own); the signaling websocket to the SFU, which stands in for the LMS server's connection on a
# datacenter link, is not shaped. Everything is written as
# soak JSONL and merged into OUTDIR/rtcd-<impl>-<arch>.jsonl for vendor/rtc/benchmarks/soak/report.py.
#
#   scripts/lesson-soak/run.sh <rs|go> <minutes> <cycle_minutes> <outdir>
#
# PROFILE=abuse (default lesson) runs the same lesson at the limit of the SFU: a background load of
# LOAD_CLASSES classes, hostile clients, churn, junk and signaling abuse from `lms-sim abuse`, and
# a network that misbehaves differently for each impaired participant, all deterministic from SEED
# (default: today's date; printed and written to OUTDIR/seed.txt). See README.md, "Abuse profile".
# The exit status is non-zero when the SFU crashed or a lesson container failed.
#
# Settings by environment: SOAK_DIR (the shared tools, default vendor/rtc/benchmarks/soak),
# SFU_IMAGE / LESSON_IMAGE / CHROME_IMAGE (prebuilt images; otherwise built here), STUDENTS_CLEAN (17),
# STUDENTS_IMPAIRED (8), MEASURE (6 receivers per group measured), TALK_SECS (30), BW_DOWN_MBIT (2.5)
# and BW_UP_MBIT (1) for the bw phase's cap of each impaired participant, and, for the abuse profile,
# ABUSE_INTENSITY (1..5) and ABUSE_ONLY (a comma separated list of attack classes).
# Needs Docker (Colima is fine), python3 and, for the Go baseline, network access to Docker Hub.
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <rs|go> <minutes> <cycle_minutes> <outdir>" >&2
  exit 2
fi
IMPL=$1 MINUTES=$2 CYCLE=$3 OUT=$4
case "$IMPL" in rs | go) ;; *)
  echo "the SFU is rs or go" >&2
  exit 2
  ;;
esac
PROFILE=${PROFILE:-lesson}
case "$PROFILE" in lesson | abuse) ;; *)
  echo "PROFILE is lesson or abuse" >&2
  exit 2
  ;;
esac
SEED=${SEED:-$(date +%Y%m%d)}
LOAD_CLASSES=${LOAD_CLASSES:-0}

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
# shellcheck source=scripts/lesson-soak/lib.sh disable=SC2034,SC2154
. "$here/lib.sh"
STUDENTS_CLEAN=${STUDENTS_CLEAN:-17}
STUDENTS_IMPAIRED=${STUDENTS_IMPAIRED:-8}
TOTAL=$((STUDENTS_CLEAN + STUDENTS_IMPAIRED))
MEASURE=${MEASURE:-6}
TALK_SECS=${TALK_SECS:-30}
BW_DOWN_MBIT=${BW_DOWN_MBIT:-2.5}
BW_UP_MBIT=${BW_UP_MBIT:-1}
# The first UDP port of the impaired students' blocks (four ports each; see lms-sim --udp-base).
UDP_BASE=40000
names=(soak-sfu soak-clean soak-impaired soak-chrome-clean soak-chrome-impaired soak-netem-chrome soak-abuse soak-load soak-sample)

# The per-process files are this run's; the merged file of the other SFU stays.
rm -f "$OUT"/clean.jsonl "$OUT"/impaired.jsonl "$OUT"/proc.jsonl "$OUT"/netem.jsonl "$OUT"/netem-chrome.jsonl \
  "$OUT"/abuse.jsonl "$OUT"/sfu-events.jsonl "$OUT/rtcd-$IMPL"-*.jsonl

cleanup() {
  docker rm -f "${names[@]}" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

prepare chrome
{
  echo "profile=$PROFILE"
  echo "seed=$SEED"
  echo "load_classes=$LOAD_CLASSES"
} >"$OUT/seed.txt"
echo "profile $PROFILE, seed $SEED (replay: SEED=$SEED PROFILE=$PROFILE $0 $IMPL $MINUTES $CYCLE <outdir>)"

start_sfu
start_sampler $((MINUTES * 60 + 900))

chrome() { # name client group: in the namespace of the client, for the clean group
  docker run -d --name "$1" --network "container:$2" --shm-size=1g "${client_cpu[@]}" "$CHROME_IMAGE" \
    "http://127.0.0.1:8099/?group=$3&ws=8100" >/dev/null
}

# The abuse profile: the background load is in before the lesson starts.
if [ "$PROFILE" = abuse ] && [ "$LOAD_CLASSES" -gt 0 ]; then
  load_container soak-load "$LOAD_CLASSES" $((MINUTES * 60 + 600))
  wait_load_joined soak-load "$LOAD_CLASSES"
fi

EXTRA="--teacher --first 0 --students $STUDENTS_CLEAN"
[ "$PROFILE" = abuse ] && EXTRA="$EXTRA --teacher-reconnect-every 15 --seed $SEED"
lesson soak-clean "$CLEAN_IP" clean
joined soak-clean
chrome soak-chrome-clean soak-clean clean
EXTRA="--first $STUDENTS_CLEAN --students $STUDENTS_IMPAIRED"
EXTRA="$EXTRA --udp-base $UDP_BASE --chrome-bind 0.0.0.0"
[ "$PROFILE" = abuse ] && EXTRA="$EXTRA --nat-rebind-every 30 --mtu --seed $SEED"
lesson soak-impaired "$IMPAIRED_IP" impaired --cap-add NET_ADMIN -v "$SOAK_DIR":/soak:ro
joined soak-impaired
# The impaired Chromium has a network namespace of its own (so that it is shaped on its own, like each
# synthetic student), and reaches the page and the signaling relay of the lesson on its address.
docker run -d --name soak-chrome-impaired --network "$NET" --ip "$CHROME_IMPAIRED_IP" --shm-size=1g \
  "${client_cpu[@]}" "$CHROME_IMAGE" "http://$IMPAIRED_IP:8099/?group=impaired&ws=8100" >/dev/null
# The impaired participants start misbehaving once they are in, each on its own: eth0 of the
# lesson container by port block, eth0 of Chromium's namespace for all its UDP, both ways.
shape=(-e NETEM_BW_DOWN_MBIT="$BW_DOWN_MBIT" -e NETEM_BW_UP_MBIT="$BW_UP_MBIT")
shape_lesson=("${shape[@]}") shape_chrome=("${shape[@]}")
chrome_netem_out=/dev/null
if [ "$PROFILE" = abuse ]; then
  student_names=""
  for ((i = STUDENTS_CLEAN; i < TOTAL; i++)); do student_names="$student_names$(printf 's%02d ' "$i")"; done
  # A seed of its own for Chromium, so that its schedule is not a copy of a student's.
  shape_lesson+=(-e NETEM_PROFILE=abuse -e NETEM_SEED="$SEED" -e NETEM_NAMES="${student_names% }")
  shape_chrome+=(-e NETEM_PROFILE=abuse -e NETEM_SEED="$((SEED + 1000))" -e NETEM_NAMES=chrome-impaired -v "$OUT":/out)
  chrome_netem_out=/out/netem-chrome.jsonl
fi
docker run -d --name soak-netem-chrome --network container:soak-chrome-impaired --cap-add NET_ADMIN \
  -v "$SOAK_DIR":/soak:ro "${shape_chrome[@]}" -e NETEM_PORTS=udp "$LESSON_IMAGE" \
  bash /soak/netem.sh eth0 "$MINUTES" "$CYCLE" "$chrome_netem_out" >/dev/null
docker exec -d "${shape_lesson[@]}" -e NETEM_PORTS="$UDP_BASE:$STUDENTS_IMPAIRED" soak-impaired \
  bash /soak/netem.sh eth0 "$MINUTES" "$CYCLE" /out/netem.jsonl
# The hostile, churning and junk-sending participants share the call, on an address of their own
# that is not shaped.
if [ "$PROFILE" = abuse ]; then
  abuse_opts=()
  [ -n "${ABUSE_INTENSITY:-}" ] && abuse_opts+=(--intensity "$ABUSE_INTENSITY")
  [ -n "${ABUSE_ONLY:-}" ] && abuse_opts+=(--only "$ABUSE_ONLY")
  docker run -d --name soak-abuse --network "$NET" --ip "$ABUSE_IP" "${client_cpu[@]}" --ulimit nofile=65536:65536 \
    -v "$OUT":/out -e LMS_SIM_LOCAL_IP="$ABUSE_IP" "$LESSON_IMAGE" \
    lms-sim abuse --http "http://$SFU_IP:8045" --media "$SFU_IP:8443" --minutes "$MINUTES" \
    --impl "rtcd-$IMPL" --seed "$SEED" --out /out/abuse.jsonl ${abuse_opts[@]+"${abuse_opts[@]}"} >/dev/null
fi
echo "lesson soak $IMPL ($ARCH), profile $PROFILE: $MINUTES min, cycle $CYCLE min, $TOTAL students + 2 Chromium"

# Until the clients are done (they leave on their own after MINUTES), or the SFU is gone.
deadline=$(($(date +%s) + (MINUTES + 10) * 60))
while docker ps -q -f name='^soak-clean$' | grep -q .; do
  if [ "$(docker inspect -f '{{.State.Running}}' soak-sfu)" != true ]; then
    echo "the SFU stopped: $(docker inspect -f 'exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' soak-sfu)"
    break
  fi
  [ "$(date +%s)" -ge "$deadline" ] && { echo "deadline passed"; break; }
  sleep 10
done
docker stop -t 20 soak-chrome-clean soak-chrome-impaired soak-netem-chrome soak-clean soak-impaired soak-abuse soak-load >/dev/null 2>&1 || true
docker stop -t 5 soak-sample >/dev/null 2>&1 || true
# The whole log of every container (the SFU's is the evidence of what it did), compressed when large.
logs=(soak-sfu soak-clean soak-impaired soak-chrome-clean soak-chrome-impaired soak-netem-chrome)
[ "$PROFILE" = abuse ] && logs+=(soak-abuse soak-load)
for n in "${logs[@]}"; do
  docker logs "$n" >"$OUT/$n.log" 2>&1 || true
done

# A crash fails the run (the SFU's events), and so does a lesson container that failed (or had to
# be stopped for its deadline); the artifacts are written either way.
failed=0
sfu_check || failed=1
for n in soak-clean soak-impaired; do
  code=$(docker inspect -f '{{.State.ExitCode}}' "$n")
  [ "$code" = 0 ] || { echo "the lesson container $n failed (exit $code)"; failed=1; }
done
if [ "$PROFILE" = abuse ] && ! grep -Eqs '"kind": ?"attack"' "$OUT/abuse.jsonl"; then
  echo "the abuse clients left no attack rows"
  failed=1
fi
for n in "${logs[@]}"; do
  if [ "$(wc -c <"$OUT/$n.log")" -gt 10485760 ]; then gzip -f "$OUT/$n.log"; fi
done

# One file per SFU: every line stamped with the architecture, in time order.
python3 - "$OUT" "rtcd-$IMPL" "$ARCH" <<'PY'
import glob, json, os, sys
out, impl, arch = sys.argv[1:4]
rows = []
for path in sorted(glob.glob(os.path.join(out, "*.jsonl"))):
    if os.path.basename(path).startswith("rtcd-"):
        continue
    for line in open(path, encoding="utf-8", errors="replace"):
        try:
            row = json.loads(line)
        except ValueError:
            continue
        row.setdefault("arch", arch)
        rows.append(row)
rows.sort(key=lambda r: r.get("ts", 0))
with open(os.path.join(out, f"{impl}-{arch}.jsonl"), "w") as f:
    for r in rows:
        f.write(json.dumps(r) + "\n")
counts = {}
for r in rows:
    key = f'{r.get("impl")}:{r.get("kind")}'
    counts[key] = counts.get(key, 0) + 1
print(json.dumps(counts, indent=1))
PY
exit "$failed"
