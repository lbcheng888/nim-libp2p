#!/usr/bin/env bash
set -euo pipefail

SSH_TARGET="${SSH_TARGET:-root@64.176.84.12}"
PUBLIC_HOST="${PUBLIC_HOST:-64-176-84-12.sslip.io}"
HEADSCALE_VERSION="${HEADSCALE_VERSION:-v0.28.0}"
HEADSCALE_USER="${HEADSCALE_USER:-nimlibp2p}"
AUTH_EXPIRATION="${AUTH_EXPIRATION:-720h}"
DERP_REGION_ID="${DERP_REGION_ID:-901}"
DERP_REGION_CODE="${DERP_REGION_CODE:-sin}"
DERP_REGION_NAME="${DERP_REGION_NAME:-Vultr Singapore}"
OUT_FILE=""

usage() {
  cat <<'EOF'
setup_headscale_derp.sh [options]

Options:
  --ssh root@HOST
  --public-host HOSTNAME
  --headscale-version v0.28.0
  --user USERNAME
  --auth-expiration 720h
  --region-id N
  --region-code CODE
  --region-name NAME
  --out FILE
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ssh) SSH_TARGET="${2:?missing ssh target}"; shift 2 ;;
    --public-host) PUBLIC_HOST="${2:?missing public host}"; shift 2 ;;
    --headscale-version) HEADSCALE_VERSION="${2:?missing headscale version}"; shift 2 ;;
    --user) HEADSCALE_USER="${2:?missing user}"; shift 2 ;;
    --auth-expiration) AUTH_EXPIRATION="${2:?missing auth expiration}"; shift 2 ;;
    --region-id) DERP_REGION_ID="${2:?missing region id}"; shift 2 ;;
    --region-code) DERP_REGION_CODE="${2:?missing region code}"; shift 2 ;;
    --region-name) DERP_REGION_NAME="${2:?missing region name}"; shift 2 ;;
    --out) OUT_FILE="${2:?missing out file}"; shift 2 ;;
    --help|-h) usage; exit 0 ;;
    *) echo "[setup_headscale_derp] unknown arg: $1" >&2; usage >&2; exit 2 ;;
  esac
done

tmp_script="$(mktemp -t nim_libp2p_headscale.XXXXXX.sh)"
cleanup_local() {
  rm -f "${tmp_script}"
}
trap cleanup_local EXIT

cat > "${tmp_script}" <<'REMOTE'
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

HS_PUBLIC_HOST="${1:?missing HS_PUBLIC_HOST}"
HS_VERSION="${2:?missing HS_VERSION}"
HS_USER="${3:?missing HS_USER}"
HS_AUTH_EXPIRATION="${4:?missing HS_AUTH_EXPIRATION}"
HS_DERP_REGION_ID="${5:?missing HS_DERP_REGION_ID}"
HS_DERP_REGION_CODE="${6:?missing HS_DERP_REGION_CODE}"
HS_DERP_REGION_NAME="${7:?missing HS_DERP_REGION_NAME}"

apt-get update -y >/dev/null
apt-get install -y ca-certificates curl python3 >/dev/null

go_bin="$(command -v go || true)"
if [[ -z "${go_bin}" && -x /snap/bin/go ]]; then
  go_bin="/snap/bin/go"
fi
if [[ -z "${go_bin}" ]]; then
  echo '{"ok":false,"error":"missing_go_binary"}'
  exit 1
fi

GOBIN=/usr/local/bin "${go_bin}" install "github.com/juanfont/headscale/cmd/headscale@${HS_VERSION}"

mkdir -p /etc/headscale /var/lib/headscale/cache

ipv4="$(ip -4 -o addr show scope global | awk '{print $4}' | head -n1 | cut -d/ -f1)"
ipv6="$(ip -6 -o addr show scope global | awk '{print $4}' | head -n1 | cut -d/ -f1)"

cat > /etc/headscale/config.yaml <<EOF
server_url: https://${HS_PUBLIC_HOST}
listen_addr: 0.0.0.0:443
metrics_listen_addr: 127.0.0.1:9090
grpc_listen_addr: 127.0.0.1:50443
grpc_allow_insecure: false

noise:
  private_key_path: /var/lib/headscale/noise_private.key

prefixes:
  v4: 100.64.0.0/10
  v6: fd7a:115c:a1e0::/48
  allocation: sequential

derp:
  server:
    enabled: true
    region_id: ${HS_DERP_REGION_ID}
    region_code: "${HS_DERP_REGION_CODE}"
    region_name: "${HS_DERP_REGION_NAME}"
    verify_clients: true
    stun_listen_addr: "0.0.0.0:3478"
    private_key_path: /var/lib/headscale/derp_server_private.key
    automatically_add_embedded_derp_region: true
    ipv4: ${ipv4}
    ipv6: ${ipv6}
  urls: []
  paths: []
  auto_update_enabled: false

disable_check_updates: true
ephemeral_node_inactivity_timeout: 30m

database:
  type: sqlite
  sqlite:
    path: /var/lib/headscale/db.sqlite
    write_ahead_log: true

acme_url: https://acme-v02.api.letsencrypt.org/directory
acme_email: ""
tls_letsencrypt_hostname: "${HS_PUBLIC_HOST}"
tls_letsencrypt_cache_dir: /var/lib/headscale/cache
tls_letsencrypt_challenge_type: HTTP-01
tls_letsencrypt_listen: ":http"

log:
  level: info
  format: text

policy:
  mode: file
  path: /etc/headscale/policy.hujson

dns:
  override_local_dns: false
  magic_dns: false
  nameservers:
    global: []
EOF

cat > /etc/headscale/policy.hujson <<'EOF'
{
  "acls": [
    {
      "action": "accept",
      "src": ["*"],
      "dst": ["*:*"]
    }
  ]
}
EOF

cat > /etc/systemd/system/headscale.service <<'EOF'
[Unit]
Description=Headscale control plane with embedded DERP
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/local/bin/headscale serve
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

ufw allow 80/tcp >/dev/null || true
ufw allow 443/tcp >/dev/null || true
ufw allow 3478/udp >/dev/null || true

/usr/local/bin/headscale configtest >/dev/null
systemctl daemon-reload
systemctl enable --now headscale >/dev/null

for _ in $(seq 1 60); do
  if systemctl is-active --quiet headscale; then
    break
  fi
  sleep 1
done
systemctl is-active --quiet headscale

for _ in $(seq 1 90); do
  if curl -skI "https://${HS_PUBLIC_HOST}" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

user_json="$(/usr/local/bin/headscale users list -o json -n "${HS_USER}")"
user_id="$(
  python3 - "${user_json}" "${HS_USER}" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1] or "[]")
name = sys.argv[2]
for row in payload if isinstance(payload, list) else []:
    if isinstance(row, dict) and str(row.get("name", "")) == name:
        print(row.get("id", ""))
        raise SystemExit(0)
raise SystemExit(1)
PY
)" || true

if [[ -z "${user_id}" ]]; then
  /usr/local/bin/headscale users create "${HS_USER}" >/dev/null
  user_json="$(/usr/local/bin/headscale users list -o json -n "${HS_USER}")"
  user_id="$(
    python3 - "${user_json}" "${HS_USER}" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1] or "[]")
name = sys.argv[2]
for row in payload if isinstance(payload, list) else []:
    if isinstance(row, dict) and str(row.get("name", "")) == name:
        print(row.get("id", ""))
        raise SystemExit(0)
raise SystemExit(1)
PY
  )"
fi

preauth_json="$(/usr/local/bin/headscale preauthkeys create -o json -u "${user_id}" --reusable -e "${HS_AUTH_EXPIRATION}")"
auth_key="$(
  python3 - "${preauth_json}" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1] or "{}")
if isinstance(payload, dict):
    for key in ("key", "preauthkey", "preAuthKey"):
        value = payload.get(key)
        if value:
            print(value)
            raise SystemExit(0)
if isinstance(payload, list):
    for row in payload:
        if not isinstance(row, dict):
            continue
        for key in ("key", "preauthkey", "preAuthKey"):
            value = row.get(key)
            if value:
                print(value)
                raise SystemExit(0)
raise SystemExit(1)
PY
)"

cat <<EOF
{
  "ok": true,
  "publicHost": "${HS_PUBLIC_HOST}",
  "controlUrl": "https://${HS_PUBLIC_HOST}",
  "user": "${HS_USER}",
  "authKey": "${auth_key}",
  "regionId": ${HS_DERP_REGION_ID},
  "regionCode": "${HS_DERP_REGION_CODE}",
  "regionName": "${HS_DERP_REGION_NAME}"
}
EOF
REMOTE

chmod +x "${tmp_script}"

remote_output="$(
  ssh "${SSH_TARGET}" \
    "bash -s -- '${PUBLIC_HOST}' '${HEADSCALE_VERSION}' '${HEADSCALE_USER}' '${AUTH_EXPIRATION}' '${DERP_REGION_ID}' '${DERP_REGION_CODE}' '${DERP_REGION_NAME}'" \
    < "${tmp_script}"
)"

if [[ -n "${OUT_FILE}" ]]; then
  mkdir -p "$(dirname "${OUT_FILE}")"
  printf '%s\n' "${remote_output}" > "${OUT_FILE}"
fi

printf '%s\n' "${remote_output}"
