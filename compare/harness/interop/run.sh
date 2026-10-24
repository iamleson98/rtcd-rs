#!/bin/bash
# Runs the interop scenarios against a running rtcd (Go or Rust) with the original Go service
# client and pion as the browser side.
#
#   harness/interop/run.sh -url http://127.0.0.1:8045
#
# Extra arguments go to the program (see -help). The build is done in a temporary module, so
# nothing is written to the Go sources.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
SRC=$(cd "${RTCD_GO_SRC:-$HERE/../../rtcd-go}" && pwd)
WORK=${INTEROP_WORK:-$(mktemp -d)}
mkdir -p "$WORK"
cp "$HERE"/*.go "$HERE"/*.html "$WORK"/
cat > "$WORK/go.mod" <<MOD
module interop

go 1.27.0

require github.com/mattermost/rtcd v0.0.0

replace github.com/mattermost/rtcd => $SRC
replace github.com/pion/interceptor v0.1.44 => github.com/bgardner8008/interceptor v0.1.44-mm-mods
replace github.com/pion/ice/v4 => github.com/bgardner8008/ice/v4 v4.2.0-role-conflict-fix-v4
replace github.com/iamleson98/sitename/server/public => $SRC/third_party/sitename-public
MOD
cp "$SRC/go.sum" "$WORK/go.sum"
cd "$WORK"
GOFLAGS=-mod=mod go build -o interop . 
exec ./interop "$@"
