#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ANDROID_ROOT="${ANDROID_ROOT:-/Users/lbcheng/UniMaker/android}"
WORK_DIR="${WORK_DIR:-$REPO_ROOT/build/mac-android-prod-ui-$(date +%Y%m%d-%H%M%S)}"
ADB_BIN="${ADB_BIN:-$(command -v adb || true)}"
ADB_SERIAL="${ADB_SERIAL:-}"
PACKAGE_NAME="${PACKAGE_NAME:-com.unimaker.native.debug}"
ACTIVITY_NAME="${ACTIVITY_NAME:-com.unimaker.nativeapp.MainActivity}"
HOST_BIN="${HOST_BIN:-$REPO_ROOT/build/tsnet_acceptance_harness_prod}"
HOST_LABEL="${HOST_LABEL:-mac-android-prod-ui-net}"
CONTROL_URL="${CONTROL_URL:-quic://64.176.84.12:9444/nim-tsnet-control-quic/v1}"
CONTROL_ENDPOINT="${CONTROL_ENDPOINT:-$CONTROL_URL}"
RELAY_ENDPOINT="${RELAY_ENDPOINT:-quic://64.176.84.12:9446/nim-tsnet-relay-quic/v1}"
CALLBACK_PORT="${CALLBACK_PORT:-39211}"
WAIT_TIMEOUT_SEC="${WAIT_TIMEOUT_SEC:-180}"
CONNECT_TIMEOUT_MS="${CONNECT_TIMEOUT_MS:-45000}"
ANDROID_TIMEOUT_MS="${ANDROID_TIMEOUT_MS:-45000}"
INSTALL_ANDROID="${INSTALL_ANDROID:-0}"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --adb-serial <serial>
  --work-dir <path>
  --control-url <quic-url>
  --control-endpoint <quic-url>
  --relay-endpoint <quic-url>
  --callback-port <port>
  --install-android
  -h, --help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --adb-serial)
      ADB_SERIAL="$2"
      shift 2
      ;;
    --work-dir)
      WORK_DIR="$2"
      shift 2
      ;;
    --control-url)
      CONTROL_URL="$2"
      shift 2
      ;;
    --control-endpoint)
      CONTROL_ENDPOINT="$2"
      shift 2
      ;;
    --relay-endpoint)
      RELAY_ENDPOINT="$2"
      shift 2
      ;;
    --callback-port)
      CALLBACK_PORT="$2"
      shift 2
      ;;
    --install-android)
      INSTALL_ANDROID=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -z "$ADB_BIN" ]]; then
  echo "adb not found" >&2
  exit 1
fi

if [[ -z "$ADB_SERIAL" ]]; then
  ADB_SERIAL="$("$ADB_BIN" devices | awk 'NR>1 && $2=="device"{print $1; exit}')"
fi
if [[ -z "$ADB_SERIAL" ]]; then
  echo "no adb device available" >&2
  exit 1
fi

mkdir -p "$WORK_DIR"

HOST_STDOUT="$WORK_DIR/host.stdout.log"
HOST_STDERR="$WORK_DIR/host.stderr.log"
CALLBACK_JSONL="$WORK_DIR/android-callback.jsonl"
CALLBACK_READY="$WORK_DIR/callback.ready"
CALLBACK_SUMMARY="$WORK_DIR/android-callback-summary.json"
LOGCAT_FILE="$WORK_DIR/android.logcat.txt"
SUMMARY_FILE="$WORK_DIR/summary.json"
RUNSTAMP="$(date +%Y%m%d-%H%M%S)"
WAIT_TOKEN="android-ui-dm-wait-$RUNSTAMP"
SEND_TOKEN="android-ui-dm-send-$RUNSTAMP"
MESSAGE_ID="android-ui-dm-$RUNSTAMP"
PING_BODY="acceptance-ping|android-prod-ui"
REPLY_BODY="acceptance-reply|$MESSAGE_ID|$HOST_LABEL"

encode_b64() {
  printf '%s' "$1" | base64 | tr -d '\n'
}

cleanup() {
  if [[ -n "${HOST_PID:-}" ]]; then
    kill "$HOST_PID" >/dev/null 2>&1 || true
  fi
  if [[ -n "${CALLBACK_PID:-}" ]]; then
    kill "$CALLBACK_PID" >/dev/null 2>&1 || true
  fi
  "$ADB_BIN" -s "$ADB_SERIAL" reverse --remove "tcp:$CALLBACK_PORT" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[mac-android-prod-ui] workDir=$WORK_DIR"
echo "[mac-android-prod-ui] adbSerial=$ADB_SERIAL"

if [[ ! -x "$HOST_BIN" ]]; then
  echo "[mac-android-prod-ui] build host harness"
  nim c \
    --hints:off \
    --warnings:off \
    -d:ssl \
    -d:libp2p_msquic_experimental \
    -d:libp2p_msquic_builtin \
    -o:"$HOST_BIN" \
    "$REPO_ROOT/examples/mobile_ffi/tsnet_acceptance_harness.nim"
fi

if [[ "$INSTALL_ANDROID" == "1" ]]; then
  echo "[mac-android-prod-ui] install android debug app"
  (
    cd "$ANDROID_ROOT"
    export JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 25)}"
    export PATH="$JAVA_HOME/bin:$PATH"
    ./gradlew :app:installDebug
  )
fi

python3 -u - "$CALLBACK_PORT" "$CALLBACK_JSONL" "$CALLBACK_READY" "$CALLBACK_SUMMARY" "$WAIT_TOKEN" "$SEND_TOKEN" "$WAIT_TIMEOUT_SEC" <<'PY' &
import json, socket, sys, time
from pathlib import Path

port = int(sys.argv[1])
jsonl_path = Path(sys.argv[2])
ready_path = Path(sys.argv[3])
summary_path = Path(sys.argv[4])
wait_token = sys.argv[5]
send_token = sys.argv[6]
timeout_sec = int(sys.argv[7])

jsonl_path.parent.mkdir(parents=True, exist_ok=True)
results = {}
stages = []
deadline = time.time() + timeout_sec

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", port))
    server.listen()
    server.settimeout(1.0)
    ready_path.write_text("ready", encoding="utf-8")
    while time.time() < deadline:
        if wait_token in results and send_token in results:
            break
        try:
            conn, _addr = server.accept()
        except socket.timeout:
            continue
        with conn:
            conn.settimeout(2.0)
            chunks = []
            while True:
                try:
                    data = conn.recv(4096)
                except socket.timeout:
                    break
                if not data:
                    break
                chunks.append(data)
            payload = b"".join(chunks).decode("utf-8", "replace").strip()
            for line in payload.splitlines():
                line = line.strip()
                if not line:
                    continue
                with jsonl_path.open("a", encoding="utf-8") as fh:
                    fh.write(line)
                    fh.write("\n")
                try:
                    node = json.loads(line)
                except Exception:
                    continue
                token = str(node.get("token", "")).strip()
                kind = str(node.get("kind", "")).strip()
                if kind == "stage":
                    stages.append(node)
                elif kind == "result" and token:
                    results[token] = node

summary = {
    "waitToken": wait_token,
    "sendToken": send_token,
    "results": results,
    "stages": stages,
}
summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
PY
CALLBACK_PID=$!

for _ in $(seq 1 30); do
  [[ -f "$CALLBACK_READY" ]] && break
  sleep 1
done
if [[ ! -f "$CALLBACK_READY" ]]; then
  echo "[mac-android-prod-ui] callback listener did not become ready" >&2
  exit 1
fi

"$ADB_BIN" -s "$ADB_SERIAL" reverse --remove "tcp:$CALLBACK_PORT" >/dev/null 2>&1 || true
"$ADB_BIN" -s "$ADB_SERIAL" reverse "tcp:$CALLBACK_PORT" "tcp:$CALLBACK_PORT" >/dev/null
"$ADB_BIN" -s "$ADB_SERIAL" logcat -c
"$ADB_BIN" -s "$ADB_SERIAL" shell am force-stop "$PACKAGE_NAME" >/dev/null 2>&1 || true

echo "[mac-android-prod-ui] start host responder"
"$HOST_BIN" \
  --role=responder \
  --label="$HOST_LABEL" \
  --controlUrl="$CONTROL_URL" \
  --controlProtocol=nim_quic \
  --controlEndpoint="$CONTROL_ENDPOINT" \
  --relayEndpoint="$RELAY_ENDPOINT" \
  --authKey= \
  --hostname="$HOST_LABEL" \
  --stateDir="$WORK_DIR/host-state" \
  --dataDir="$WORK_DIR/host-data" \
  --waitTimeoutSec="$WAIT_TIMEOUT_SEC" \
  --connectTimeoutMs="$CONNECT_TIMEOUT_MS" \
  --quicOnly=true \
  --emitLocalInfoStdout=true \
  --emitResultStdout=true \
  --noFileOutput=true \
  >"$HOST_STDOUT" 2>"$HOST_STDERR" &
HOST_PID=$!

HOST_LOCAL_B64=""
for _ in $(seq 1 "$WAIT_TIMEOUT_SEC"); do
  if [[ -f "$HOST_STDOUT" ]]; then
    HOST_LOCAL_B64="$( (grep '^TSNET_LOCAL_INFO_JSON_B64=' "$HOST_STDOUT" || true) | tail -n1 | sed 's/^TSNET_LOCAL_INFO_JSON_B64=//' )"
    if [[ -n "$HOST_LOCAL_B64" ]]; then
      break
    fi
  fi
  sleep 1
done
if [[ -z "$HOST_LOCAL_B64" ]]; then
  echo "[mac-android-prod-ui] host local_info not emitted" >&2
  exit 1
fi

HOST_PEER_ID="$(python3 - "$HOST_LOCAL_B64" <<'PY'
import base64, json, sys
node = json.loads(base64.b64decode(sys.argv[1]).decode("utf-8"))
print(node.get("peerId", ""))
PY
)"
HOST_SEED_ADDRS_JSON="$(python3 - "$HOST_LOCAL_B64" <<'PY'
import base64, json, sys
node = json.loads(base64.b64decode(sys.argv[1]).decode("utf-8"))
print(json.dumps(node.get("listenAddrs", []), ensure_ascii=False, separators=(",", ":")))
PY
)"
if [[ -z "$HOST_PEER_ID" || "$HOST_SEED_ADDRS_JSON" == "[]" ]]; then
  echo "[mac-android-prod-ui] host local_info missing peer or addrs" >&2
  exit 1
fi

echo "[mac-android-prod-ui] launch android ui_dm_wait"
"$ADB_BIN" -s "$ADB_SERIAL" shell am start -W --activity-single-top --activity-no-animation \
  -n "$PACKAGE_NAME/$ACTIVITY_NAME" \
  --es auto_cmd ui_dm_wait \
  --es auto_token "$WAIT_TOKEN" \
  --es peer_id "$HOST_PEER_ID" \
  --es body_b64 "$(encode_b64 "$REPLY_BODY")" \
  --es message_id "$MESSAGE_ID" \
  --es seed_peer_id "$HOST_PEER_ID" \
  --es seed_multiaddrs_b64 "$(encode_b64 "$HOST_SEED_ADDRS_JSON")" \
  --es seed_source android_prod_ui_dm \
  --es auto_callback_host 127.0.0.1 \
  --es auto_callback_port "$CALLBACK_PORT" \
  --es auto_disable_file_output true \
  --es auto_callback_stages true \
  --ei timeoutMs "$ANDROID_TIMEOUT_MS" >/dev/null

sleep 2

echo "[mac-android-prod-ui] launch android ui_dm_send"
"$ADB_BIN" -s "$ADB_SERIAL" shell am start -W --activity-single-top --activity-no-animation \
  -n "$PACKAGE_NAME/$ACTIVITY_NAME" \
  --es auto_cmd ui_dm_send \
  --es auto_token "$SEND_TOKEN" \
  --es peer_id "$HOST_PEER_ID" \
  --es body_b64 "$(encode_b64 "$PING_BODY")" \
  --es message_id "$MESSAGE_ID" \
  --es seed_peer_id "$HOST_PEER_ID" \
  --es seed_multiaddrs_b64 "$(encode_b64 "$HOST_SEED_ADDRS_JSON")" \
  --es seed_source android_prod_ui_dm \
  --es auto_callback_host 127.0.0.1 \
  --es auto_callback_port "$CALLBACK_PORT" \
  --es auto_disable_file_output true \
  --es auto_callback_stages true \
  --es holdAfterMs 1500 \
  --ei timeoutMs "$ANDROID_TIMEOUT_MS" >/dev/null

wait "$CALLBACK_PID"
unset CALLBACK_PID

HOST_RESULT_B64=""
for _ in $(seq 1 60); do
  if [[ -f "$HOST_STDOUT" ]]; then
    HOST_RESULT_B64="$( (grep '^TSNET_RESULT_JSON_B64=' "$HOST_STDOUT" || true) | tail -n1 | sed 's/^TSNET_RESULT_JSON_B64=//' )"
    if [[ -n "$HOST_RESULT_B64" ]]; then
      break
    fi
  fi
  if ! kill -0 "$HOST_PID" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

HOST_EXIT=0
if kill -0 "$HOST_PID" >/dev/null 2>&1; then
  wait "$HOST_PID" || HOST_EXIT=$?
else
  wait "$HOST_PID" || HOST_EXIT=$?
fi
unset HOST_PID

"$ADB_BIN" -s "$ADB_SERIAL" logcat -d >"$LOGCAT_FILE" || true

python3 - "$CALLBACK_SUMMARY" "$HOST_RESULT_B64" "$HOST_EXIT" "$SUMMARY_FILE" "$LOGCAT_FILE" <<'PY'
import base64, json, re, sys
from pathlib import Path

callback_summary = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
host_result_b64 = sys.argv[2].strip()
host_exit = int(sys.argv[3])
summary_path = Path(sys.argv[4])
logcat_path = Path(sys.argv[5])

host_result = {}
if host_result_b64:
    try:
        host_result = json.loads(base64.b64decode(host_result_b64).decode("utf-8"))
    except Exception as exc:
        host_result = {"ok": False, "error": f"host_result_decode_failed:{exc}"}

results = callback_summary.get("results", {})
wait_token = callback_summary.get("waitToken", "")
send_token = callback_summary.get("sendToken", "")
wait_result = results.get(wait_token, {})
send_result = results.get(send_token, {})
wait_payload = wait_result.get("payload", {}) if isinstance(wait_result, dict) else {}
send_payload = send_result.get("payload", {}) if isinstance(send_result, dict) else {}

logcat = logcat_path.read_text(encoding="utf-8", errors="replace") if logcat_path.exists() else ""
crash_detected = bool(re.search(r"Fatal signal|SIGSEGV|SIGABRT|am_crash|Process .* has died", logcat))

summary = {
    "ok": bool(host_result.get("ok")) and bool(wait_payload.get("ok")) and bool(send_payload.get("ok")) and not crash_detected and host_exit == 0,
    "hostResult": host_result,
    "hostExit": host_exit,
    "androidWait": wait_payload,
    "androidSend": send_payload,
    "callbackSummary": callback_summary,
    "crashDetected": crash_detected,
}
summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps(summary, ensure_ascii=False, indent=2))
if not summary["ok"]:
    raise SystemExit(1)
PY
