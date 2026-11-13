#!/usr/bin/env bash
# The load ceiling of one SFU on this machine: classes are added to the SFU (two CPUs, as in run.sh)
# step by step while a small measured lesson runs through the ramp, until the SFU's CPU saturates
# (180 of its 200 % for 30 s) or the lesson's receivers collapse (freezes, stalls), or the ramp
# reaches RAMP_MAX_CLASSES. See ceiling.py for the rules. Writes OUTDIR/ceiling-<impl>-<arch>.json,
# which hold.py turns into the load of the abuse profile (90 % of the lower ceiling of the two SFUs).
#
#   scripts/lesson-soak/calibrate.sh <rs|go> <outdir>
#
# By environment: RAMP_STEP_SECS (60) between steps, RAMP_CLASSES_PER_STEP (1) added by each,
# RAMP_MAX_CLASSES (40), RAMP_FREEZE_MAX (6 freezes a minute per measured track), and the settings of
# lib.sh (LOAD_STUDENTS 30 per class, the images). One ramp takes at most
# ceil(RAMP_MAX_CLASSES / RAMP_CLASSES_PER_STEP) x RAMP_STEP_SECS plus about five minutes.
# The exit status is non-zero when the SFU crashed.
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 <rs|go> <outdir>" >&2
  exit 2
fi
IMPL=$1 OUT=$2
case "$IMPL" in rs | go) ;; *)
  echo "the SFU is rs or go" >&2
  exit 2
  ;;
esac
RAMP_STEP_SECS=${RAMP_STEP_SECS:-60}
RAMP_CLASSES_PER_STEP=${RAMP_CLASSES_PER_STEP:-1}
RAMP_MAX_CLASSES=${RAMP_MAX_CLASSES:-40}
RAMP_FREEZE_MAX=${RAMP_FREEZE_MAX:-6}

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)
# shellcheck source=scripts/lesson-soak/lib.sh
. "$here/lib.sh"
names=(soak-sfu soak-clean soak-load soak-sample)
steps=$(((RAMP_MAX_CLASSES + RAMP_CLASSES_PER_STEP - 1) / RAMP_CLASSES_PER_STEP))
ramp_secs=$((steps * RAMP_STEP_SECS))
# The measured lesson outlasts the ramp, its hold at the top and its start.
MINUTES=$(((ramp_secs + RAMP_STEP_SECS) / 60 + 4))
TOTAL=2 MEASURE=2 TALK_SECS=30
LESSON_CHROME=()
ceiling="$OUT/ceiling-$IMPL-$ARCH.json"
ramp_file="$OUT/calibrate-ramp-$IMPL.jsonl"

rm -f "$OUT"/clean.jsonl "$OUT"/proc.jsonl "$OUT"/sfu-events.jsonl "$ceiling" "$ramp_file" \
  "$OUT/calibrate-proc-$IMPL.jsonl" "$OUT/calibrate-clean-$IMPL.jsonl"
cleanup() {
  docker rm -f "${names[@]}" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

prepare
start_sfu
start_sampler $((ramp_secs + 600))
EXTRA="--teacher --first 0 --students 2"
lesson soak-clean "$CLEAN_IP" clean
joined soak-clean
echo "calibrate $IMPL ($ARCH): up to $RAMP_MAX_CLASSES classes of $LOAD_STUDENTS students, $RAMP_CLASSES_PER_STEP every ${RAMP_STEP_SECS}s"
docker run -d --name soak-load --network "$NET" --ip "$LOAD_IP" "${client_cpu[@]}" --ulimit nofile=65536:65536 \
  -e LMS_SIM_LOCAL_IP="$LOAD_IP" "$LESSON_IMAGE" \
  lms-sim ramp --http "http://$SFU_IP:8045" --media "$SFU_IP:8443" --step-secs "$RAMP_STEP_SECS" \
  --per-step "$RAMP_CLASSES_PER_STEP" --max-classes "$RAMP_MAX_CLASSES" --students "$LOAD_STUDENTS" \
  --cameras "$LOAD_CAMERAS" --watch "$LOAD_WATCH" --per-second "$LOAD_PER_SECOND" \
  --seconds $((ramp_secs + 600)) >/dev/null

evaluate() {
  docker logs soak-load 2>&1 | grep '^{' >"$ramp_file" || true
  python3 "$here/ceiling.py" --proc "$OUT/proc.jsonl" --recv "$OUT/clean.jsonl" --ramp "$ramp_file" \
    --per-step "$RAMP_CLASSES_PER_STEP" --freeze-max "$RAMP_FREEZE_MAX" "$@"
}
# Until the SFU collapses, or the ramp has reached its top and held it for a step, or the SFU is gone.
deadline=$(($(date +%s) + ramp_secs + 600))
top_since=0
crashed=0
while :; do
  if [ "$(docker inspect -f '{{.State.Running}}' soak-sfu)" != true ]; then
    echo "the SFU stopped during the ramp"
    crashed=1
    break
  fi
  result=$(evaluate)
  case "$result" in *'"reason": null'*) ;; *)
    echo "collapsed: $result"
    break
    ;;
  esac
  if [ "$top_since" -eq 0 ] && grep -q "\"classes\": $RAMP_MAX_CLASSES}" "$ramp_file"; then top_since=$(date +%s); fi
  if [ "$top_since" -ne 0 ] && [ "$(date +%s)" -ge $((top_since + RAMP_STEP_SECS)) ]; then
    echo "the ramp reached $RAMP_MAX_CLASSES classes: $result"
    break
  fi
  [ "$(date +%s)" -ge "$deadline" ] && { echo "deadline passed: $result"; break; }
  sleep 10
done
evaluate --write "$ceiling" --impl "$IMPL" --arch "$ARCH" --students "$LOAD_STUDENTS" >/dev/null
docker stop -t 20 soak-clean soak-load >/dev/null 2>&1 || true
docker stop -t 5 soak-sample >/dev/null 2>&1 || true
docker logs soak-sfu >"$OUT/soak-sfu-calibrate-$IMPL.log" 2>&1 || true
cp "$OUT/soak-sfu-calibrate-$IMPL.log" "$OUT/soak-sfu.log"
sfu_check || crashed=1
rm -f "$OUT/soak-sfu.log"
mv "$OUT/proc.jsonl" "$OUT/calibrate-proc-$IMPL.jsonl"
mv "$OUT/clean.jsonl" "$OUT/calibrate-clean-$IMPL.jsonl"
cat "$ceiling"
exit "$crashed"
