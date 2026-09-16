#!/usr/bin/env python3
"""Tiny local dashboard server: producers/consumers POST events here, the
browser page polls GET /state for live counts + a recent-events feed."""
import threading
import time
from collections import deque
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory

ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "dashboard"

app = Flask(__name__, static_folder=None)
LOCK = threading.Lock()

STATE = {
    "produced": {"us": 0, "eu": 0},
    "produced_by_condition": {"test": 0, "prod": 0},
    "consumed": {
        "analytics": {"us": 0, "eu": 0},
        "test": {"test": 0, "prod": 0},
        "prod": {"test": 0, "prod": 0},
    },
}
RECENT = deque(maxlen=60)


@app.route("/")
def index():
    return send_from_directory(DASHBOARD_DIR, "index.html")


@app.route("/event", methods=["POST"])
def event():
    payload = request.get_json(force=True)
    kind = payload.get("type")
    region = (payload.get("region") or "").lower()
    condition = (payload.get("condition") or "").lower()

    with LOCK:
        if kind == "produced":
            if region in STATE["produced"]:
                STATE["produced"][region] += 1
            if condition in STATE["produced_by_condition"]:
                STATE["produced_by_condition"][condition] += 1
        elif kind == "consumed":
            persona = payload.get("persona")
            bucket = STATE["consumed"].get(persona)
            if bucket is not None:
                if persona == "analytics" and region in bucket:
                    bucket[region] += 1
                elif condition in bucket:
                    bucket[condition] += 1
        RECENT.append({**payload, "ts": time.time()})

    return jsonify(ok=True)


@app.route("/state")
def state():
    with LOCK:
        produced_total = STATE["produced"]["us"] + STATE["produced"]["eu"]
        analytics_total = STATE["consumed"]["analytics"]["us"] + STATE["consumed"]["analytics"]["eu"]
        # test-consumer should only ever see condition=test; anything it received
        # tagged "prod" would mean Kong's skip_record policy failed to skip it.
        # skipped_for_test == every prod record produced, since none should reach it.
        skipped_for_test = STATE["produced_by_condition"]["prod"] - STATE["consumed"]["test"]["prod"]
        skipped_for_prod = STATE["produced_by_condition"]["test"] - STATE["consumed"]["prod"]["test"]
        return jsonify(
            produced=STATE["produced"],
            produced_by_condition=STATE["produced_by_condition"],
            consumed=STATE["consumed"],
            produced_total=produced_total,
            analytics_total=analytics_total,
            skipped_for_test=max(skipped_for_test, 0),
            skipped_for_prod=max(skipped_for_prod, 0),
            leaked_to_test=STATE["consumed"]["test"]["prod"],
            leaked_to_prod=STATE["consumed"]["prod"]["test"],
            recent=list(RECENT)[-30:],
        )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8090, debug=False)
