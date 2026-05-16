#!/bin/bash
# ─────────────────────────────────────────────────────
#  amalgame-messaging-nats — Test Runner
#  Usage: ./tests/run_tests.sh [/path/to/amc]
#
#  Probes 127.0.0.1:4222 for a NATS server. If reachable:
#  compile fixture, run, assert. Otherwise: SKIP. Start a server
#  with: docker run --rm -p 4222:4222 nats:2-alpine
#                  # or `brew install nats-server && nats-server &`
# ─────────────────────────────────────────────────────

set -u

if [ $# -ge 1 ]; then
    AMC="$1"
elif [ -n "${AMC:-}" ]; then
    :
elif command -v amc >/dev/null 2>&1; then
    AMC="$(command -v amc)"
else
    echo "ERROR: amc not found." >&2
    exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PKG_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PKG_RUNTIME="$PKG_ROOT/runtime"

AMC_DIR="$(cd "$(dirname "$AMC")" && pwd)"
if [ -d "$AMC_DIR/runtime" ]; then
    AMC_RUNTIME="$AMC_DIR/runtime"
elif [ -n "${AMC_RUNTIME:-}" ]; then
    :
else
    echo "ERROR: amc runtime/ not found. Set AMC_RUNTIME=..." >&2
    exit 2
fi

BUILD_DIR="$(mktemp -d -t anats-XXXXXX)"
trap 'rm -rf "$BUILD_DIR"' EXIT
PROJ_DIR="$BUILD_DIR/proj"
mkdir -p "$PROJ_DIR"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'
PASS=0; FAIL=0; SKIP=0

echo ""
echo "════════════════════════════════════════════"
echo "  amalgame-messaging-nats — Tests"
echo "════════════════════════════════════════════"
echo "  amc:     $AMC ($("$AMC" --version 2>&1))"
echo "  runtime: $AMC_RUNTIME"
echo ""

NATS_AVAILABLE=0
if (echo > /dev/tcp/127.0.0.1/4222) 2>/dev/null; then
    NATS_AVAILABLE=1
    echo "  nats: reachable at 127.0.0.1:4222"
else
    echo "  nats: NOT reachable at 127.0.0.1:4222 — every case will SKIP"
fi
echo ""

# ── Stage a fake cache pointing at the local working tree ──
# Without this, the test .am file's `import Amalgame.Messaging.NATS`
# fails because amc has no way to know about the package. `amc
# package add` needs a published tag, which this CI doesn't have
# until release time. So we manually craft the cache layout amc
# expects + a project amalgame.lock pointing into it.
FAKE_CACHE="$BUILD_DIR/cache"
PKG_GIT="github.com/amalgame-lang/amalgame-messaging-nats"
PKG_TAG="${PKG_TAG:-v0.1.0}"
FAKE_SHA="deadbeefcafebabe0000000000000000000000ab"
SHORT_SHA="${FAKE_SHA:0:8}"
PKG_CACHE_DIR="$FAKE_CACHE/$PKG_GIT/${PKG_TAG}_${SHORT_SHA}"

mkdir -p "$(dirname "$PKG_CACHE_DIR")"
rm -rf "$PKG_CACHE_DIR"
ln -s "$PKG_ROOT" "$PKG_CACHE_DIR"

cat > "$PROJ_DIR/amalgame.lock" <<EOF
[[package]]
name = "amalgame-messaging-nats"
git  = "$PKG_GIT"
tag  = "$PKG_TAG"
rev  = "$FAKE_SHA"
EOF

export AMALGAME_PACKAGES_DIR="$FAKE_CACHE"
echo "  cache:   $FAKE_CACHE → $PKG_ROOT"
echo ""

run_test() {
    local name="$1"
    local expected="$2"
    printf "  %-38s" "$name"
    if [ "$NATS_AVAILABLE" != "1" ]; then
        echo -e "${YELLOW}SKIP${NC} (no nats server on 127.0.0.1:4222)"
        SKIP=$((SKIP + 1)); return
    fi
    cp "$SCRIPT_DIR/stdlib_nats.am" "$PROJ_DIR/test.am"
    local out_base="$PROJ_DIR/test"
    local out
    out=$(cd "$PROJ_DIR" && "$AMC" -o test test.am 2>&1)
    if [ $? -ne 0 ]; then
        echo -e "${RED}FAIL${NC} (amc)"; echo "$out" | head -3 | sed 's/^/    /'
        FAIL=$((FAIL + 1)); return
    fi
    if [ ! -f "$out_base.c" ]; then
        echo -e "${RED}FAIL${NC} (no .c)"; FAIL=$((FAIL + 1)); return
    fi
    gcc -O2 -I"$AMC_RUNTIME" -I"$PKG_RUNTIME" "$out_base.c" \
        -lgc -lm -lcurl -ldl -lpthread -o "$out_base" 2>/dev/null
    if [ ! -x "$out_base" ]; then
        echo -e "${RED}FAIL${NC} (gcc link)"; FAIL=$((FAIL + 1)); return
    fi
    local run_output
    run_output=$("$out_base" 2>&1)
    if echo "$run_output" | grep -qF "$expected"; then
        echo -e "${GREEN}PASS${NC}"; PASS=$((PASS + 1))
    else
        echo -e "${RED}FAIL${NC}"
        echo "    expected: $expected"
        echo "    got:      $(echo "$run_output" | head -3 | tr '\n' '|')"
        FAIL=$((FAIL + 1))
    fi
}

echo "── NATS ────────────────────────────────────"
run_test "open 4222"             "[PASS] open 4222"
run_test "ping"                  "[PASS] ping"
run_test "subscribe greeting"    "[PASS] subscribe greeting"
run_test "publish greeting"      "[PASS] publish greeting"
run_test "received message"      "[PASS] received message"
run_test "subject matches"       "[PASS] subject matches"
run_test "payload matches"       "[PASS] payload matches"
run_test "wildcard subscription" "[PASS] wildcard subscription"
run_test "second message"        "[PASS] second message"
run_test "wait timeout"          "[PASS] wait timeout"
run_test "close"                 "[PASS] closed"

echo ""
echo "────────────────────────────────────────────"
echo -e "  ${GREEN}PASS: $PASS${NC}  |  ${RED}FAIL: $FAIL${NC}  |  ${YELLOW}SKIP: $SKIP${NC}"
echo "────────────────────────────────────────────"
echo ""

[ $FAIL -eq 0 ] && exit 0 || exit 1
