#!/usr/bin/env python3
"""Idempotently create the two region topics on the user's Confluent Cloud cluster."""
import os
import sys
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

TOPICS = ["orders.us", "orders.eu"]


def main():
    bootstrap = os.environ.get("BOOTSTRAP_SERVERS", "").strip()
    api_key = os.environ.get("KAFKA_API_KEY", "").strip()
    api_secret = os.environ.get("KAFKA_API_SECRET", "").strip()
    if not (bootstrap and api_key and api_secret):
        sys.exit(
            "Missing BOOTSTRAP_SERVERS / KAFKA_API_KEY / KAFKA_API_SECRET in .env — "
            "fill these in with your Confluent Cloud cluster's values first."
        )

    admin = AdminClient(
        {
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": api_key,
            "sasl.password": api_secret,
        }
    )

    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [NewTopic(t, num_partitions=3, replication_factor=3) for t in TOPICS if t not in existing]

    if not to_create:
        print("orders.us and orders.eu already exist")
        return

    futures = admin.create_topics(to_create)
    for topic, fut in futures.items():
        try:
            fut.result()
            print(f"created topic: {topic}")
        except Exception as e:
            print(f"failed to create {topic}: {e}")


if __name__ == "__main__":
    main()
