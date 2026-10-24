#!/bin/bash
# Regenerates the golden wire vectors in crates/rtcd-proto/tests/golden from the original Go rtcd.
#
#   RTCD_GO_SRC=/path/to/rtcd compat/golden/gen.sh
#
# The vectors are checked in, so CI does not need the Go sources; run this when the Go side changes
# its wire format, and commit the diff.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
SRC=$(cd "${RTCD_GO_SRC:?point RTCD_GO_SRC at the Go rtcd checkout}" && pwd)
OUT="$HERE/../../crates/rtcd-proto/tests/golden"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$OUT"
cp "$HERE/main.go" "$WORK/main.go"
cat > "$WORK/go.mod" <<MOD
module golden

go 1.27.0

require github.com/mattermost/rtcd v0.0.0

replace github.com/mattermost/rtcd => $SRC
replace github.com/pion/interceptor v0.1.44 => github.com/bgardner8008/interceptor v0.1.44-mm-mods
replace github.com/pion/ice/v4 => github.com/bgardner8008/ice/v4 v4.2.0-role-conflict-fix-v4
replace github.com/iamleson98/sitename/server/public => $SRC/third_party/sitename-public
MOD
cp "$SRC/go.sum" "$WORK/go.sum"
(cd "$WORK" && GOFLAGS=-mod=mod go run . "$OUT")
