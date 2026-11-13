#!/usr/bin/env bash
# One lesson soak against one SFU: the SFU in a container limited to 2 CPUs, the lesson in two client
# containers (a clean network and an impaired one, shaped by netem.sh), a headless Chromium in each
# client network, and a sampler that reads the SFU's process from outside. Everything is written as
# soak JSONL and merged into OUTDIR/rtcd-<impl>-<arch>.jsonl for vendor/rtc/benchmarks/soak/report.py.
#
#   scripts/lesson-soak/run.sh <rs|go> <minutes> <cycle_minutes> <outdir>
#
# Settings by environment: SOAK_DIR (the shared tools, default vendor/rtc/benchmarks/soak),
# SFU_IMAGE / LESSON_IMAGE / CHROME_IMAGE (prebuilt images; otherwise built here), STUDENTS_CLEAN (17),
# STUDENTS_IMPAIRED (8), MEASURE (6 receivers per group measured), TALK_SECS (30).
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

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
SOAK_DIR=${SOAK_DIR:-$root/vendor/rtc/benchmarks/soak}
LESSON_IMAGE=${LESSON_IMAGE:-rtcd-lesson:soak}
CHROME_IMAGE=${CHROME_IMAGE:-rtcd-lesson-chrome:soak}
STUDENTS_CLEAN=${STUDENTS_CLEAN:-17}
STUDENTS_IMPAIRED=${STUDENTS_IMPAIRED:-8}
TOTAL=$((STUDENTS_CLEAN + STUDENTS_IMPAIRED))
MEASURE=${MEASURE:-6}
TALK_SECS=${TALK_SECS:-30}
# The upstream Go service, the latest stable release when this was written (2026-06-29), by tag and
# by the digest of its multi-arch index (linux/amd64 and linux/arm64).
GO_IMAGE='mattermost/rtcd:v1.2.6@sha256:a8c3c1c3fa2937527d3a96351534fa116106b88aca777bed49d6a360fb2eacd3'
if [ "$IMPL" = rs ]; then SFU_IMAGE=${SFU_IMAGE:-rtcd-rs:soak}; else SFU_IMAGE=${SFU_IMAGE:-$GO_IMAGE}; fi
# What the lines say (and `uname -m` on a Mac says arm64).
ARCH=$(uname -m)
[ "$ARCH" = arm64 ] && ARCH=aarch64
NET=soak-net
SFU_IP=172.31.0.2 CLEAN_IP=172.31.0.3 IMPAIRED_IP=172.31.0.4
names=(soak-sfu soak-clean soak-impaired soak-chrome-clean soak-chrome-impaired soak-sample)

mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
# The per-process files are this run's; the merged file of the other SFU stays.
rm -f "$OUT"/clean.jsonl "$OUT"/impaired.jsonl "$OUT"/proc.jsonl "$OUT"/netem.jsonl "$OUT/rtcd-$IMPL"-*.jsonl

cleanup() {
  docker rm -f "${names[@]}" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

if [ "$IMPL" = rs ] && ! docker image inspect "$SFU_IMAGE" >/dev/null 2>&1; then
  docker build -t "$SFU_IMAGE" "$root"
fi
docker image inspect "$LESSON_IMAGE" >/dev/null 2>&1 || docker build -f "$here/Dockerfile" -t "$LESSON_IMAGE" "$root"
docker image inspect "$CHROME_IMAGE" >/dev/null 2>&1 || docker build -t "$CHROME_IMAGE" "$here/chrome"
docker network inspect "$NET" >/dev/null 2>&1 || docker network create --subnet 172.31.0.0/24 "$NET" >/dev/null

# The SFU gets two CPUs of its own; the clients share the rest (all of them when there are fewer
# than four, where the SFU is held to two CPUs of time instead).
ncpu=$(docker info --format '{{.NCPU}}')
if [ "$ncpu" -ge 4 ]; then
  sfu_cpu=(--cpuset-cpus=0-1)
  client_cpu=(--cpuset-cpus="2-$((ncpu - 1))")
else
  sfu_cpu=(--cpus=2)
  client_cpu=()
fi

# The same settings for both services, by the RTCD_* variables they share.
envs=(
  -e RTCD_API_HTTP_LISTENADDRESS=:8045
  -e RTCD_API_SECURITY_ALLOWSELFREGISTRATION=true
  -e RTCD_RTC_ICEHOSTOVERRIDE="$SFU_IP"
  -e RTCD_RTC_ICEPORTUDP=8443
  -e RTCD_RTC_ICEPORTTCP=8443
  -e RTCD_STORE_DATASOURCE=/tmp/rtcd_db
  -e RTCD_LOGGER_ENABLEFILE=false
  -e RTCD_LOGGER_CONSOLELEVEL=INFO
)
docker run -d --name soak-sfu --network "$NET" --ip "$SFU_IP" "${sfu_cpu[@]}" \
  --memory=2g --memory-swap=2g --ulimit nofile=65536:65536 "${envs[@]}" "$SFU_IMAGE" >/dev/null
for _ in $(seq 60); do
  docker run --rm --network "$NET" "$LESSON_IMAGE" curl -sf "http://$SFU_IP:8045/version" >/dev/null 2>&1 && break
  sleep 1
done
docker run --rm --network "$NET" "$LESSON_IMAGE" curl -sf "http://$SFU_IP:8045/version" | head -c 300
echo

pid=$(docker inspect -f '{{.State.Pid}}' soak-sfu)
docker run -d --name soak-sample --privileged --pid=host -v "$OUT":/out "$LESSON_IMAGE" \
  python3 /usr/local/bin/sample.py "$pid" "rtcd-$IMPL" /out/proc.jsonl 5 $((MINUTES * 60 + 900)) >/dev/null

lesson() { # name ip group extra-args...
  local name=$1 ip=$2 group=$3
  shift 3
  docker run -d --name "$name" --network "$NET" --ip "$ip" "${client_cpu[@]}" --ulimit nofile=65536:65536 \
    -v "$OUT":/out -e LMS_SIM_LOCAL_IP="$ip" "$@" "$LESSON_IMAGE" \
    lms-sim lesson --http "http://$SFU_IP:8045" --media "$SFU_IP:8443" --minutes "$MINUTES" \
    --impl "rtcd-$IMPL" --group "$group" --total "$TOTAL" --measure "$MEASURE" --talk-secs "$TALK_SECS" \
    --chrome 8099 --out "/out/$group.jsonl" $EXTRA >/dev/null
}
joined() { # container: waits for its "joined" line
  for _ in $(seq 120); do
    docker logs "$1" 2>&1 | grep -q '"joined"' && return 0
    docker ps -q -f name="^$1$" | grep -q . || { docker logs "$1" 2>&1 | tail -5; return 1; }
    sleep 1
  done
  return 1
}
chrome() { # name client group
  docker run -d --name "$1" --network "container:$2" --shm-size=1g "${client_cpu[@]}" "$CHROME_IMAGE" \
    "http://127.0.0.1:8099/?group=$3&ws=8100" >/dev/null
}

EXTRA="--teacher --first 0 --students $STUDENTS_CLEAN"
lesson soak-clean "$CLEAN_IP" clean
joined soak-clean
chrome soak-chrome-clean soak-clean clean
EXTRA="--first $STUDENTS_CLEAN --students $STUDENTS_IMPAIRED"
lesson soak-impaired "$IMPAIRED_IP" impaired --cap-add NET_ADMIN -v "$SOAK_DIR":/soak:ro
joined soak-impaired
chrome soak-chrome-impaired soak-impaired impaired
# The impaired network starts misbehaving once its students are in; eth0 is shaped in both directions.
docker exec -d soak-impaired bash /soak/netem.sh eth0 "$MINUTES" "$CYCLE" /out/netem.jsonl
echo "lesson soak $IMPL ($ARCH): $MINUTES min, cycle $CYCLE min, $TOTAL students + 2 Chromium"

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
docker stop -t 20 soak-chrome-clean soak-chrome-impaired soak-clean soak-impaired >/dev/null 2>&1 || true
docker stop -t 5 soak-sample >/dev/null 2>&1 || true
state=$(docker inspect -f 'exit={{.State.ExitCode}} oom={{.State.OOMKilled}} running={{.State.Running}}' soak-sfu)
echo "SFU: $state"
for n in soak-sfu soak-clean soak-impaired soak-chrome-clean soak-chrome-impaired; do
  docker logs "$n" 2>&1 | tail -n 300 >"$OUT/$n.log" || true
done

# One file per SFU: every line stamped with the architecture, in time order.
python3 - "$OUT" "rtcd-$IMPL" "$ARCH" "$state" <<'PY'
import glob, json, os, sys, time
out, impl, arch, state = sys.argv[1:5]
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
if "running=false" in state and "exit=0" not in state:
    rows.append({"ts": int(time.time() * 1000), "impl": impl, "arch": arch, "kind": "event", "id": "sfu", "what": "error", "detail": state})
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
