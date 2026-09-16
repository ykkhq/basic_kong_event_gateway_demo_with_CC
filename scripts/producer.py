#!/usr/bin/env python3
"""Regional order producer. Publishes to the 'orders' topic alias — it has no
idea orders.us / orders.eu even exist; Kong Event Gateway routes by identity."""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from common import dashboard_url, load_runtime_config

CUSTOMERS = [f"cust-{i:04d}" for i in range(1, 200)]
CURRENCIES = {"us": "USD", "eu": "EUR"}
TEST_TRAFFIC_RATIO = 0.2  # ~20% of orders are synthetic/canary test traffic


def make_order(region: str) -> dict:
    condition = "test" if random.random() < TEST_TRAFFIC_RATIO else "prod"
    return {
        "order_id": str(uuid.uuid4()),
        "region": region.upper(),
        "condition": condition,
        "customer_id": random.choice(CUSTOMERS),
        "amount": round(random.uniform(9.99, 499.99), 2),
        "currency": CURRENCIES[region],
        "item_count": random.randint(1, 6),
        "status": random.choice(["placed", "placed", "placed", "pending_review"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def report(region: str, order: dict):
    try:
        requests.post(
            f"{dashboard_url()}/event",
            json={"type": "produced", "region": region, **order},
            timeout=1,
        )
    except requests.RequestException:
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", choices=["us", "eu"], required=True)
    args = parser.parse_args()

    cfg = load_runtime_config()
    vc = cfg["orders_vc"]
    creds = vc["producers"][args.region]

    producer = Producer(
        {
            "bootstrap.servers": f"bootstrap.{vc['dns_label']}.localhost:{vc['port']}",
            "security.protocol": "SASL_SSL",
            "ssl.endpoint.identification.algorithm": "none",
            "enable.ssl.certificate.verification": False,
            "sasl.mechanism": "PLAIN",
            "sasl.username": creds["username"],
            "sasl.password": creds["password"],
        }
    )

    print(f"[producer-{args.region}] publishing to alias 'orders' as {creds['username']}")
    while True:
        order = make_order(args.region)
        producer.produce(
            "orders",
            key=order["order_id"],
            value=json.dumps(order),
            headers={"region": order["region"], "condition": order["condition"]},
            callback=lambda err, msg, o=order: print(
                f"[producer-{args.region}] {'FAILED ' + str(err) if err else 'sent ' + o['order_id']}"
            ),
        )
        producer.poll(0)
        report(args.region, order)
        time.sleep(random.uniform(1, 5))


if __name__ == "__main__":
    main()
