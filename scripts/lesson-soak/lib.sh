# shellcheck shell=bash disable=SC2034,SC2154
# What run.sh (a lesson or an abuse soak) and calibrate.sh (the load ramp) share: the images, the
# network and its addresses, the SFU in a container of two CPUs, its sampler, the lesson client
# containers, the background load and the evidence of a crash. Sourced, never run. The caller sets
# IMPL (rs|go), OUT (an existing absolute directory), here and root (this directory and the
# repository) before it calls anything.

SOAK_DIR=${SOAK_DIR:-$root/vendor/rtc/benchmarks/soak}
LESSON_IMAGE=${LESSON_IMAGE:-rtcd-lesson:soak}
CHROME_IMAGE=${CHROME_IMAGE:-rtcd-lesson-chrome:soak}
# The upstream Go service, the latest stable release when this was written (2026-06-29), by tag and
# by the digest of its multi-arch index (linux/amd64 and linux/arm64).
GO_IMAGE='mattermost/rtcd:v1.2.6@sha256:a8c3c1c3fa2937527d3a96351534fa116106b88aca777bed49d6a360fb2eacd3'
if [ "$IMPL" = rs ]; then SFU_IMAGE=${SFU_IMAGE:-rtcd-rs:soak}; else SFU_IMAGE=${SFU_IMAGE:-$GO_IMAGE}; fi
# What the lines say (and `uname -m` on a Mac says arm64).
ARCH=$(uname -m)
[ "$ARCH" = arm64 ] && ARCH=aarch64
NET=soak-net
SFU_IP=172.31.0.2 CLEAN_IP=172.31.0.3 IMPAIRED_IP=172.31.0.4 ABUSE_IP=172.31.0.5
CHROME_IMPAIRED_IP=172.31.0.6 LOAD_IP=172.31.0.7
# A class of the background load and of the calibration ramp (`lms-sim load` and `ramp`).
LOAD_STUDENTS=${LOAD_STUDENTS:-30}
LOAD_CAMERAS=${LOAD_CAMERAS:-4}
LOAD_WATCH=${LOAD_WATCH:-2}
LOAD_PER_SECOND=${LOAD_PER_SECOND:-20}
# The option that lets a lesson container serve Chromium; calibrate.sh has no browser.
LESSON_CHROME=(--chrome 8099)

# The architecture, the kernel, the cores and the CPU, for the artifacts.
machine() {
  {
    echo "arch: $ARCH (host uname -m: $(uname -m))"
    docker info --format 'docker: {{.ServerVersion}}, {{.NCPU}} cpus, {{.MemTotal}} bytes'
    docker run --rm "$LESSON_IMAGE" sh -c 'echo "kernel: $(uname -sr)"; echo "cores: $(nproc)"; grep -m3 -iE "^(model name|hardware|cpu implementer|cpu part)" /proc/cpuinfo || true'
  } >"$OUT/machine.txt"
}

# The images (built here when they are not prebuilt), the artifacts' machine.txt and the network.
# $1 = chrome when the browser image is needed.
prepare() {
  if [ "$IMPL" = rs ] && ! docker image inspect "$SFU_IMAGE" >/dev/null 2>&1; then
    docker build -t "$SFU_IMAGE" "$root"
  fi
  docker image inspect "$LESSON_IMAGE" >/dev/null 2>&1 || docker build -f "$here/Dockerfile" -t "$LESSON_IMAGE" "$root"
  if [ "${1:-}" = chrome ]; then
    docker image inspect "$CHROME_IMAGE" >/dev/null 2>&1 || docker build -t "$CHROME_IMAGE" "$here/chrome"
  fi
  machine
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
}

# Turns off UDP segmentation offload on the SFU's interface, for both services alike. rtcd-rs sends a
# peer's datagrams as one GSO super-packet, which the shaped downlink (netem on ifb) would drop and
# count as a single packet; with the offload off the stack splits it into the datagrams a real link
# carries. The setting is recorded in machine.txt; a failure stops the soak rather than skew it.
segment_in_software() {
  local state
  state=$(docker run --rm --network container:soak-sfu --cap-add NET_ADMIN "$LESSON_IMAGE"     sh -c 'ethtool -K eth0 tx-udp-segmentation off && ethtool -k eth0 | grep -i segmentation') ||
    { echo "cannot turn off UDP segmentation offload on soak-sfu's eth0" >&2; return 1; }
  case "$state" in *"tx-udp-segmentation: off"*) ;; *) echo "UDP segmentation offload is still on: $state" >&2; return 1 ;; esac
  { echo "sfu eth0 segmentation offload:"; echo "$state"; } >>"$OUT/machine.txt"
}

# The SFU in a container named soak-sfu, with the same settings for both services by the RTCD_*
# variables they share, and waits until it answers.
start_sfu() {
  local envs=(
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
    --memory=4g --memory-swap=4g --ulimit nofile=65536:65536 "${envs[@]}" "$SFU_IMAGE" >/dev/null
  segment_in_software
  for _ in $(seq 60); do
    docker run --rm --network "$NET" "$LESSON_IMAGE" curl -sf "http://$SFU_IP:8045/version" >/dev/null 2>&1 && break
    sleep 1
  done
  docker run --rm --network "$NET" "$LESSON_IMAGE" curl -sf "http://$SFU_IP:8045/version" | head -c 300
  echo
}

# The sampler of the SFU's process, into OUT/proc.jsonl, for $1 seconds.
start_sampler() {
  local pid
  pid=$(docker inspect -f '{{.State.Pid}}' soak-sfu)
  docker run -d --name soak-sample --privileged --pid=host -v "$OUT":/out "$LESSON_IMAGE" \
    python3 /usr/local/bin/sample.py "$pid" "rtcd-$IMPL" /out/proc.jsonl 5 "$1" >/dev/null
}

# A lesson client container: name ip group extra-docker-args...; the lesson's own options are in
# EXTRA, and MINUTES, TOTAL, MEASURE and TALK_SECS are the caller's.
# shellcheck disable=SC2086 # EXTRA is a list of options
lesson() {
  local name=$1 ip=$2 group=$3
  shift 3
  docker run -d --name "$name" --network "$NET" --ip "$ip" "${client_cpu[@]}" --ulimit nofile=65536:65536 \
    -v "$OUT":/out -e LMS_SIM_LOCAL_IP="$ip" "$@" "$LESSON_IMAGE" \
    lms-sim lesson --http "http://$SFU_IP:8045" --media "$SFU_IP:8443" --minutes "$MINUTES" \
    --impl "rtcd-$IMPL" --group "$group" --total "$TOTAL" --measure "$MEASURE" --talk-secs "$TALK_SECS" \
    "${LESSON_CHROME[@]}" --out "/out/$group.jsonl" $EXTRA >/dev/null
}
joined() { # container: waits for its "joined" line
  for _ in $(seq 120); do
    docker logs "$1" 2>&1 | grep -q '"joined"' && return 0
    docker ps -q -f name="^$1$" | grep -q . || { docker logs "$1" 2>&1 | tail -5; return 1; }
    sleep 1
  done
  return 1
}

# The background classes of `lms-sim load`, held for $3 seconds in a container named $1: name classes
# seconds. wait_load_joined then waits until every participant of them has joined.
load_container() {
  local name=$1 classes=$2 seconds=$3
  docker run -d --name "$name" --network "$NET" --ip "$LOAD_IP" "${client_cpu[@]}" --ulimit nofile=65536:65536 \
    -e LMS_SIM_LOCAL_IP="$LOAD_IP" "$LESSON_IMAGE" \
    lms-sim load --http "http://$SFU_IP:8045" --media "$SFU_IP:8443" --classes "$classes" \
    --students "$LOAD_STUDENTS" --cameras "$LOAD_CAMERAS" --watch "$LOAD_WATCH" \
    --per-second "$LOAD_PER_SECOND" --seconds "$seconds" >/dev/null
}
wait_load_joined() { # container classes
  local limit=$((($2 * (LOAD_STUDENTS + 1)) / LOAD_PER_SECOND + 120))
  for _ in $(seq "$limit"); do
    docker logs "$1" 2>&1 | grep -q '"all": true' && return 0
    docker ps -q -f name="^$1$" | grep -q . || { docker logs "$1" 2>&1 | tail -5; return 1; }
    sleep 1
  done
  return 1
}

# The evidence that the SFU crashed, from its container and its whole log (OUT/soak-sfu.log, which
# the caller has written): it stopped, was killed for memory, was restarted, or its log has a panic or
# a fatal error. One `event` row per finding goes to OUT/sfu-events.jsonl (the report counts them and
# its gate is zero), and the return status is 1 when there is any. The container never restarts by
# itself (no restart policy), so a restart count is a finding too.
sfu_check() {
  local running exit oom restarts hits status=0
  read -r running exit oom restarts < <(docker inspect -f '{{.State.Running}} {{.State.ExitCode}} {{.State.OOMKilled}} {{.RestartCount}}' soak-sfu)
  echo "SFU: running=$running exit=$exit oom=$oom restarts=$restarts"
  finding() {
    python3 - "$OUT/sfu-events.jsonl" "rtcd-$IMPL" "$ARCH" "$1" <<'PY'
import json, sys, time
path, impl, arch, detail = sys.argv[1:5]
with open(path, "a") as f:
    f.write(json.dumps({"ts": int(time.time() * 1000), "impl": impl, "arch": arch, "kind": "event", "id": "sfu", "what": "error", "detail": detail}) + "\n")
PY
    echo "SFU finding: $1" >&2
    status=1
  }
  [ "$running" = true ] || finding "the SFU container stopped (exit $exit)"
  [ "$oom" = false ] || finding "the SFU container was killed for memory (OOMKilled)"
  [ "$restarts" = 0 ] || finding "the SFU container restarted $restarts times"
  hits=$(grep -ciE 'panic|panicked at|fatal error|SIGSEGV|SIGABRT' "$OUT/soak-sfu.log" 2>/dev/null || true)
  if [ "${hits:-0}" -gt 0 ]; then
    finding "$hits log lines with a panic or a fatal error, the first: $(grep -iE -m1 'panic|panicked at|fatal error|SIGSEGV|SIGABRT' "$OUT/soak-sfu.log" | cut -c1-300)"
  fi
  return "$status"
}
