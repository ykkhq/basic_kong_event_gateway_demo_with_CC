#!/usr/bin/env bash
# Stops all running producer.py / consumer.py demo processes.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "== stopping producers =="
pkill -f "scripts/producer.py" && echo "stopped" || echo "none running"

echo "== stopping consumers =="
pkill -f "scripts/consumer.py" && echo "stopped" || echo "none running"
