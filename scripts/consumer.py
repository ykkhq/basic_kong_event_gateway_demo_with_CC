#!/usr/bin/env python3
"""Order consumer. 'analytics' sees everything, unfiltered — the raw firehose.
'test' and 'prod' subscribe to the exact same two topics, but each has its
own skip_record policy on its virtual cluster: 'test' never receives a
condition=prod record and 'prod' never receives a condition=test record.
Synthetic/canary test traffic is mixed into the real order stream at low
volume, and Kong keeps it from ever reaching the production consumer (and
vice versa) without either consumer writing a line of filtering code."""
import argparse
import json

import requests
from confluent_kafka import Consumer

from common import dashboard_url, load_runtime_config

VC_BY_PERSONA = {
    "analytics": ("orders_vc", "analytics_consumer", "analytics-group"),
    "test": ("test_vc", "consumer", "test-group"),
    "prod": ("prod_vc", "consumer", "prod-group"),
}


def report(persona: str, region: str, condition: str, order: dict):
    try:
        requests.post(
            f"{dashboard_url()}/event",
            json={"type": "consumed", "persona": persona, "region": region, "condition": condition, **order},
            timeout=1,
        )
    except requests.RequestException:
        pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--persona", choices=["analytics", "test", "prod"], required=True)
    args = parser.parse_args()

    cfg = load_runtime_config()
    vc_key, creds_key, group_id = VC_BY_PERSONA[args.persona]
    vc = cfg[vc_key]
    creds = vc[creds_key]

    consumer = Consumer(
        {
            "bootstrap.servers": f"bootstrap.{vc['dns_label']}.localhost:{vc['port']}",
            "security.protocol": "SASL_SSL",
            "ssl.endpoint.identification.algorithm": "none",
            "enable.ssl.certificate.verification": False,
            "sasl.mechanism": "PLAIN",
            "sasl.username": creds["username"],
            "sasl.password": creds["password"],
            "group.id": group_id,
            "auto.offset.reset": "latest",
        }
    )
    topics = [cfg["topics"]["us"], cfg["topics"]["eu"]]
    consumer.subscribe(topics)
    print(f"[consumer-{args.persona}] subscribed to {topics} as {creds['username']}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            order = json.loads(msg.value())
            region = order.get("region", "?")
            condition = order.get("condition", "?")
            print(f"[consumer-{args.persona}] received {region}/{condition} order {order['order_id']}")
            report(args.persona, region, condition, order)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
