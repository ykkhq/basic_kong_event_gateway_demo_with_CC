#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Provisions (or reuses) the dedicated Konnect Gateway Control Plane that lets
# classic Kong run in this compose stack as a Konnect-managed hybrid-mode data
# plane — with NO KONG_LICENSE_DATA. openid-connect / opa / ai-custom-guardrail
# / the hcv Vault backend are all Enterprise-gated on kong/kong-ai-gateway:2.0.3
# and refuse to load with an empty license; registering as a Konnect DP grants
# the license via that connection instead (same pattern as the ai-gateway-dp
# service, and as the production k8s deployment — see docker/README.md §6).
#
# This CP is separate from BOTH the k8s deployment's `bankong` CP and the
# `bankong-ai-gateway` AI Gateway CP (docker/scripts/ai-quickstart.sh) — a
# dedicated CP per deployment target, matching this repo's existing pattern.
#
# Usage:
#   KONNECT_TOKEN=kpat_... bash docker/scripts/kong-cp-setup.sh [cp-name]
#
# Idempotent: reuses the CP if one with this name already exists, and reuses
# the local cert pair if kong/certs/ already has one (delete it first to
# rotate). Prints the docker/.env values to set, and the `deck gateway sync`
# command to push docker/kong/kong.yaml to the CP.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."   # docker/scripts/.. -> docker/

# use user stored kpat
#KONNECT_TOKEN=$(cat ~/.kong/kpat)

if [ -n "${KONNECT_TOKEN+x}" ]; then
    echo "check ~/.kong/kpat file."
    KONNECT_TOKEN=$(cat ~/.kong/kpat)
fi

if [ -z "$KONNECT_TOKEN" ]; then
    echo "configure KONNECT_TOKEN. For example, do > export KONNECT_TOKEN=kpat_...............  "
    exit 1
fi


CP_NAME="${1:-${KONNECT_KONG_CP_NAME:-event-gateway-demo}}"
REGION="${KONNECT_REGION:-us}"
BASE_URL="https://${REGION}.api.konghq.com"
: "${KONNECT_TOKEN:?Set KONNECT_TOKEN (a Konnect personal access token) first — see docker/.env}"

echo "== looking for existing control plane '$CP_NAME' in $REGION =="
CP_JSON=$(curl -sf "$BASE_URL/v2/control-planes?filter%5Bname%5D%5Beq%5D=$CP_NAME" \
  -H "Authorization: Bearer $KONNECT_TOKEN")
CP_ID=$(echo "$CP_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print((d.get('data') or [{}])[0].get('id',''))")

if [ -z "$CP_ID" ]; then
  echo "== creating control plane '$CP_NAME' =="
  CP_JSON=$(curl -sf -X POST "$BASE_URL/v2/control-planes" \
    -H "Authorization: Bearer $KONNECT_TOKEN" -H "Content-Type: application/json" \
    -d "{\"name\":\"$CP_NAME\",\"description\":\" Konnect-managed so no KONG_LICENSE_DATA is needed\",\"cluster_type\":\"CLUSTER_TYPE_CONTROL_PLANE\",\"labels\":{\"project\":\"bankong\",\"env\":\"docker-compose\"}}")
  CP_ID=$(echo "$CP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
else
  echo "   found existing CP $CP_ID — reusing it"
  CP_JSON=$(curl -sf "$BASE_URL/v2/control-planes/$CP_ID" -H "Authorization: Bearer $KONNECT_TOKEN")
fi

CP_HOST=$(echo "$CP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['config']['control_plane_endpoint'].removeprefix('https://'))")
TP_HOST=$(echo "$CP_JSON" | python3 -c "import sys,json; print(json.load(sys.stdin)['config']['telemetry_endpoint'].removeprefix('https://'))")

mkdir -p kong/certs
if [ ! -f kong/certs/tls.crt ] || [ ! -f kong/certs/key.crt ]; then
  echo "== generating self-signed DP client cert =="
  openssl req -new -x509 -nodes -newkey rsa:2048 \
    -keyout kong/certs/key.crt -out kong/certs/tls.crt \
    -days 3650 -subj "/CN=${CP_NAME}-kong-dp"
  echo "== registering cert with Konnect =="
  python3 -c "import json; print(json.dumps({'cert': open('kong/certs/tls.crt').read()}))" \
    | curl -sf -X POST "$BASE_URL/v2/control-planes/$CP_ID/dp-client-certificates" \
        -H "Authorization: Bearer $KONNECT_TOKEN" -H "Content-Type: application/json" \
        -d @- >/dev/null
else
  echo "== kong/certs/ already has a cert pair — reusing it (delete to rotate) =="
fi

cat <<EOF

Control plane ready: $CP_NAME ($CP_ID)

Set these in docker/.env:
  KONNECT_KONG_CP_NAME=$CP_NAME
  KONG_CP_HOST=$CP_HOST
  KONG_TP_HOST=$TP_HOST

Then push docker/kong/kong.yaml to it (do this any time you change kong.yaml):
  export DECK_KONNECT_TOKEN="\$KONNECT_TOKEN"
  deck gateway sync --konnect-addr $BASE_URL --konnect-control-plane-name $CP_NAME kong/kong.yaml

Then bring kong up: docker compose up -d kong
EOF
