#!/usr/bin/env bash
# The remote client of a lesson soak: one headless Chromium on this machine, across the internet from
# the SFU that `REMOTE_CLIENTS=1 PUBLIC_IP=<sfu-public-ip> scripts/lesson-soak/run.sh ...` started. It is
# the same container image and page as the local clients' (the picture score included), with no network
# shaping: the path is whatever lies between the two machines.
#
#   scripts/lesson-soak/remote-client.sh <sfu-public-ip> <minutes> <outdir>
#
# The browser joins the lesson as one more student through the relay of the SFU host (the page on
# port 8099, the websocket on 8100), and its media goes straight to the SFU's published port 8443 (udp
# or tcp). What it measures goes back over that websocket, so the SFU host writes it, with the other
# clients', to <its outdir>/remote.jsonl (impl `rtcd-rs/chrome-remote`, group `remote`) and into the merged
# file. This machine's <outdir> gets the browser's own log (chrome-remote.log: whether the picture
# score is on, the page's errors). Needs Docker and this directory (the image is built from chrome/).
# Start it after run.sh has printed "remote client:"; the browser stops after <minutes> (the soak's).
# Settings by environment: CHROME_IMAGE (a prebuilt image; default rtcd-lesson-chrome:soak, built here
# when missing), WAIT_SECS (how long to wait for the SFU host's page, default 300).
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <sfu-public-ip> <minutes> <outdir>" >&2
  exit 2
fi
SFU=$1 MINUTES=$2 OUT=$3
case "$MINUTES" in '' | *[!0-9]*)
  echo "minutes is a whole number" >&2
  exit 2
  ;;
esac
here=$(cd "$(dirname "$0")" && pwd)
CHROME_IMAGE=${CHROME_IMAGE:-rtcd-lesson-chrome:soak}
WAIT_SECS=${WAIT_SECS:-300}
NAME=soak-remote-chrome
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd)

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

docker image inspect "$CHROME_IMAGE" >/dev/null 2>&1 || docker build -t "$CHROME_IMAGE" "$here/chrome"

# The page is served by the SFU host's lesson process once the soak has started. It is asked for by a
# container of the browser's image, from the network the browser will be on.
page="http://$SFU:8099/"
answers() {
  docker run --rm --entrypoint node "$CHROME_IMAGE" -e \
    'fetch(process.argv[1], { signal: AbortSignal.timeout(3000) }).then((r) => process.exit(r.ok ? 0 : 1), () => process.exit(1))' "$page"
}
deadline=$(($(date +%s) + WAIT_SECS))
until answers; do
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "$page did not answer in ${WAIT_SECS}s (is run.sh up with REMOTE_CLIENTS=1, and port 8099 open to this machine?)" >&2
    exit 1
  fi
  sleep 2
done

docker run -d --name "$NAME" --shm-size=1g "$CHROME_IMAGE" "${page}?group=remote&ws=8100" >/dev/null
echo "Chromium joined from this machine to $SFU for $MINUTES min"
deadline=$(($(date +%s) + MINUTES * 60))
status=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  if [ "$(docker inspect -f '{{.State.Running}}' "$NAME")" != true ]; then
    echo "the browser stopped early (exit $(docker inspect -f '{{.State.ExitCode}}' "$NAME"))" >&2
    status=1
    break
  fi
  sleep 5
done
docker stop -t 20 "$NAME" >/dev/null 2>&1 || true
docker logs "$NAME" >"$OUT/chrome-remote.log" 2>&1 || true
echo "log: $OUT/chrome-remote.log"
exit "$status"
