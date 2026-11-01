#!/usr/bin/env bash
# One run of the overload lab: rtcd in one container with a real kernel send path (a token bucket
# on its interface, so the qdisc drops what the card would), the load generator in another.
#
#   scripts/overload-lab/run.sh IMAGE LABEL [ENV=VALUE ...]
#
# Settings by environment: RATE_MBIT (100), QDISC_KB (1024), MEM (3g), CLASSES (4), STUDENTS (60),
# CAMERAS (8), WATCH (3), PER_SECOND (6), HOLD (600, seconds the people stay once all joined),
# SAMPLE (5, seconds between samples). The arguments after LABEL are RTCD_* settings for the node.
# Output: out/LABEL.jsonl (the node sampled from inside), out/LABEL.gen.log, out/LABEL.rtcd.log.
set -euo pipefail
cd "$(dirname "$0")"
IMAGE=${1:?image}
LABEL=${2:?label}
shift 2
RATE_MBIT=${RATE_MBIT:-100}
QDISC_KB=${QDISC_KB:-1024}
MEM=${MEM:-3g}
CLASSES=${CLASSES:-4}
STUDENTS=${STUDENTS:-60}
CAMERAS=${CAMERAS:-8}
WATCH=${WATCH:-3}
PER_SECOND=${PER_SECOND:-6}
HOLD=${HOLD:-600}
SAMPLE=${SAMPLE:-5}
ASSETS=${ASSETS:-${TMPDIR:-/tmp}/lms-sim-assets}
NET=rtcd-lab
RTCD_IP=172.30.0.2
GEN_IP=172.30.0.3
mkdir -p out

docker network inspect "$NET" >/dev/null 2>&1 || docker network create --subnet 172.30.0.0/24 "$NET" >/dev/null
docker rm -f lab-rtcd lab-gen >/dev/null 2>&1 || true

envs=(-e "RTCD_ICE_HOST_OVERRIDE=$RTCD_IP" -e RTCD_LOG_LEVEL=WARN)
for kv in "$@"; do envs+=(-e "$kv"); done
docker run -d --name lab-rtcd --network "$NET" --ip "$RTCD_IP" --cpuset-cpus=0-2 \
  --memory="$MEM" --memory-swap="$MEM" --cap-add NET_ADMIN --ulimit nofile=65536:65536 \
  "${envs[@]}" "$IMAGE" rtcd-entrypoint.sh >/dev/null
until docker exec lab-rtcd curl -sf localhost:8045/version >/dev/null 2>&1; do sleep 1; done
docker exec lab-rtcd tc qdisc replace dev eth0 root tbf rate "${RATE_MBIT}mbit" burst 128kb limit "${QDISC_KB}kb"
docker exec -d lab-rtcd sh -c "python3 /usr/local/bin/sample.py $SAMPLE 100000 > /data/sample.jsonl"

total=$(( CLASSES * (STUDENTS + 1) ))
docker run -d --name lab-gen --network "$NET" --ip "$GEN_IP" --cpuset-cpus=3-5 \
  -v "$ASSETS":/assets:ro -e LMS_SIM_ASSETS=/assets -e "LMS_SIM_LOCAL_IP=$GEN_IP" \
  --ulimit nofile=65536:65536 "$IMAGE" lms-sim load --http "http://$RTCD_IP:8045" \
  --media "$RTCD_IP:8443" --classes "$CLASSES" --students "$STUDENTS" --cameras "$CAMERAS" \
  --watch "$WATCH" --per-second "$PER_SECOND" --seconds "$HOLD" >/dev/null
echo "run $LABEL: $total people, $RATE_MBIT Mbit/s, $MEM, joined at $PER_SECOND/s, then $HOLD s"

# Until the generator is done, or the node is gone (killed: OOM).
while docker ps --format '{{.Names}}' | grep -qx lab-gen; do
  if [ "$(docker inspect -f '{{.State.OOMKilled}}' lab-rtcd)" = true ] || ! docker ps --format '{{.Names}}' | grep -qx lab-rtcd; then
    echo "the node was killed (OOMKilled=$(docker inspect -f '{{.State.OOMKilled}}' lab-rtcd))"
    break
  fi
  sleep 5
done
docker cp lab-rtcd:/data/sample.jsonl "out/$LABEL.jsonl" 2>/dev/null || true
docker logs lab-gen >"out/$LABEL.gen.log" 2>&1 || true
docker logs lab-rtcd >"out/$LABEL.rtcd.log" 2>&1 || true
docker inspect -f 'exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' lab-rtcd | tee "out/$LABEL.exit"
docker rm -f lab-rtcd lab-gen >/dev/null 2>&1 || true
