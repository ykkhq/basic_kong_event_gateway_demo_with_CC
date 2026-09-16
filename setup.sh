#!/usr/bin/env bash
# Runs the full initial setup for the orders demo: venv + deps, Confluent
# Cloud topics, then Konnect Event Gateway config (creates the gateway itself
# if needed, starts/restarts the local Docker data-plane, backend cluster,
# listener, virtual clusters, routing, ACLs, skip_record policies). Idempotent
# — safe to re-run any time, including after a colima/docker restart.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

if [ ! -d .venv ]; then
  echo "== creating venv =="
  python3 -m venv .venv
fi
source .venv/bin/activate

echo "== installing dependencies =="
pip install -q -r requirements.txt

echo "== creating Confluent Cloud topics =="
python scripts/create_confluent_topics.py

echo "== configuring Kong Event Gateway =="
python scripts/setup_konnect.py

echo
echo "Setup complete. Next:"
echo "  source .venv/bin/activate"
echo "  python scripts/dashboard_server.py &"
echo "  python scripts/producer.py --region us &"
echo "  python scripts/producer.py --region eu &"
echo "  python scripts/consumer.py --persona analytics &"
echo "  python scripts/consumer.py --persona test &"
echo "  python scripts/consumer.py --persona prod &"
echo "  open http://127.0.0.1:8090"
