#!/bin/sh
# Entrypoint for the LMS rtcd image.
#
# Renders /etc/rtcd/config.template.toml into /tmp/rtcd/config.toml using
# RTCD_* environment variables, then execs the rtcd binary. This keeps the
# service definition env-driven: changing the advertised ICE IP or STUN/TURN
# servers is a service update, not an image rebuild.
#
# Values are substituted with sed; the template therefore only accepts
# values without the sentinel "__" and without "/" where the sed pattern
# below uses "|" as delimiter — all supported options (IPs, URLs, ports,
# booleans, paths) satisfy this except ICE server lists, which use "," as
# the separator (no "/" in URLs would break it: stun:/turn: URLs are fine).
set -eu

TEMPLATE="/etc/rtcd/config.template.toml"
OUT_DIR="/tmp/rtcd"
OUT="${OUT_DIR}/config.toml"

render_bool() {
    # Normalizes 1/true/yes/on (any case) to "true", everything else "false".
    case "$(echo "$1" | tr '[:upper:]' '[:lower:]')" in
        1|true|yes|on) echo "true" ;;
        *) echo "false" ;;
    esac
}

RTCD_API_LISTEN="${RTCD_API_LISTEN:-:8045}"
RTCD_ALLOW_SELF_REGISTRATION="$(render_bool "${RTCD_ALLOW_SELF_REGISTRATION:-true}")"
RTCD_ENABLE_ADMIN="$(render_bool "${RTCD_ENABLE_ADMIN:-false}")"
RTCD_ICE_PORT_UDP="${RTCD_ICE_PORT_UDP:-8443}"
RTCD_ICE_PORT_TCP="${RTCD_ICE_PORT_TCP:-8443}"
RTCD_ICE_HOST_OVERRIDE="${RTCD_ICE_HOST_OVERRIDE:-}"
RTCD_ICE_SERVERS="${RTCD_ICE_SERVERS:-stun:stun.l.google.com:19302}"
RTCD_DATA_SOURCE="${RTCD_DATA_SOURCE:-/data/rtcd_db}"
RTCD_LOG_LEVEL="${RTCD_LOG_LEVEL:-INFO}"
RTCD_LOG_JSON="$(render_bool "${RTCD_LOG_JSON:-false}")"

# Admin secret: env var wins, then a mounted secret file, then disabled.
RTCD_ADMIN_SECRET="${RTCD_ADMIN_SECRET:-}"
if [ -z "$RTCD_ADMIN_SECRET" ] && [ -r "${RTCD_ADMIN_SECRET_FILE:-/run/secrets/rtcd_admin_secret}" ]; then
    RTCD_ADMIN_SECRET="$(tr -d '\r\n' < "${RTCD_ADMIN_SECRET_FILE:-/run/secrets/rtcd_admin_secret}")"
fi

mkdir -p "$OUT_DIR" "$(dirname "$RTCD_DATA_SOURCE")"

if [ "$RTCD_ENABLE_ADMIN" = "true" ]; then
    if [ -z "$RTCD_ADMIN_SECRET" ]; then
        echo "rtcd: RTCD_ENABLE_ADMIN=true requires RTCD_ADMIN_SECRET or rtcd_admin_secret" >&2
        exit 1
    fi
    ADMIN_LINE="admin_secret_key = \"${RTCD_ADMIN_SECRET}\""
else
    ADMIN_LINE=""
fi

# Validate numeric ports (defense against typos in the service env).
for p in "$RTCD_ICE_PORT_UDP" "$RTCD_ICE_PORT_TCP"; do
    case "$p" in
        ''|*[!0-9]*) echo "rtcd: invalid ICE port '${p}'" >&2; exit 1 ;;
    esac
done

sed \
    -e "s|__RTCD_API_LISTEN__|${RTCD_API_LISTEN}|g" \
    -e "s|__RTCD_ALLOW_SELF_REGISTRATION__|${RTCD_ALLOW_SELF_REGISTRATION}|g" \
    -e "s|__RTCD_ENABLE_ADMIN__|${RTCD_ENABLE_ADMIN}|g" \
    -e "s|__RTCD_ADMIN_SECRET_LINE__|${ADMIN_LINE}|g" \
    -e "s|__RTCD_ICE_PORT_UDP__|${RTCD_ICE_PORT_UDP}|g" \
    -e "s|__RTCD_ICE_PORT_TCP__|${RTCD_ICE_PORT_TCP}|g" \
    -e "s|__RTCD_ICE_HOST_OVERRIDE__|${RTCD_ICE_HOST_OVERRIDE}|g" \
    -e "s|__RTCD_ICE_SERVERS__|${RTCD_ICE_SERVERS}|g" \
    -e "s|__RTCD_DATA_SOURCE__|${RTCD_DATA_SOURCE}|g" \
    -e "s|__RTCD_LOG_LEVEL__|${RTCD_LOG_LEVEL}|g" \
    -e "s|__RTCD_LOG_JSON__|${RTCD_LOG_JSON}|g" \
    "$TEMPLATE" > "$OUT"

# Fail fast on an unrendered placeholder (guards template/value drift).
if grep -q '__RTCD_' "$OUT"; then
    echo "rtcd: config template still contains placeholders after rendering:" >&2
    grep -n '__RTCD_' "$OUT" >&2
    exit 1
fi

chmod 0600 "$OUT"

echo "rtcd: rendered config (${OUT}) api=${RTCD_API_LISTEN} ice_udp=${RTCD_ICE_PORT_UDP} ice_tcp=${RTCD_ICE_PORT_TCP} host_override='${RTCD_ICE_HOST_OVERRIDE}'"

exec /usr/local/bin/rtcd -config "$OUT"
