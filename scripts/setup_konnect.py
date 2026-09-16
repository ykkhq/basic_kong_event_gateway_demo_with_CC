#!/usr/bin/env python3
"""Idempotently configure the Kong Event Gateway for the multi-region orders demo.

Creates the Event Gateway itself if it doesn't exist yet (unlike Kong's own
quickstart script, this never deletes/recreates an existing one), a Konnect
DP client certificate (kept in kong/dp-certs/, reused across runs) and makes
sure the local Docker data-plane container is running against it — this is
what died after a colima/docker restart and needs restarting here, not on
Konnect's side.

Creates:
  - 1 backend cluster -> the user's Confluent Cloud Kafka cluster
  - 1 listener (port 19092) with:
      - a tls_server policy (self-signed cert, TLS required)
      - a forward_to_virtual_cluster/sni policy, so ALL virtual clusters below
        share this one port, distinguished by TLS SNI (dns_label + ".localhost")
  - 3 virtual clusters:
      orders-vc   (producers: us/eu, + global-analytics-consumer — unfiltered firehose)
      test-vc     (test-consumer — only ever receives condition=test records)
      prod-vc     (prod-consumer — only ever receives condition=prod records)
  - topic_aliases on orders-vc routing the "orders" alias to orders.us / orders.eu
    based on which principal is producing (context.auth.principal.name)
  - ACL policies granting each virtual cluster's principals exactly the access they need
  - a skip_record consume policy on test-vc dropping any record whose "condition"
    header isn't "test", and the mirror-image policy on prod-vc dropping anything
    whose "condition" header isn't "prod" — synthetic/canary test traffic is mixed
    into the real order stream at low volume, and Kong keeps each consumer from
    ever seeing the other's data.

Bootstrap hostnames use the ".localhost" TLD (RFC 6761 — always resolves to
loopback, no /etc/hosts needed): bootstrap.orders.localhost / bootstrap.test.localhost / bootstrap.prod.localhost.

Writes connection details + generated credentials to runtime_config.json.
"""
import json
import os
import secrets
import subprocess
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

EVENT_GATEWAY_NAME = "orders-demo-event-gateway"
KONNECT_REGION = os.environ.get("KONNECT_REGION", "us")
BASE_URL = f"https://{KONNECT_REGION}.api.konghq.com/v1"

TOPIC_US = "orders.us"
TOPIC_EU = "orders.eu"
GATEWAY_PORT = 19092
CERT_DIR = ROOT / "kong" / "listener-certs"
DP_CERT_DIR = ROOT / "kong" / "dp-certs"
DP_CONTAINER_NAME = "orders-event-gateway-dp"
DP_IMAGE = "kong/kong-event-gateway:latest"


def kpat() -> str:
    path = Path.home() / ".kong" / "kpat"
    if not path.exists():
        sys.exit(f"Konnect PAT not found at {path}")
    return path.read_text().strip()


SESSION = requests.Session()


def api(method: str, path: str, **kwargs):
    resp = SESSION.request(
        method,
        f"{BASE_URL}{path}",
        headers={"Authorization": f"Bearer {kpat()}", "Content-Type": "application/json"},
        **kwargs,
    )
    if resp.status_code >= 400:
        sys.exit(f"{method} {path} -> {resp.status_code}: {resp.text}")
    return resp.json() if resp.text else {}


def find_one(items: list, **match):
    for item in items:
        if all(item.get(k) == v for k, v in match.items()):
            return item
    return None


def get_or_create_event_gateway_id() -> str:
    data = [gw for gw in api("GET", "/event-gateways")["data"] if gw["name"] == EVENT_GATEWAY_NAME]
    if data:
        return data[0]["id"]
    created = api(
        "POST",
        "/event-gateways",
        json={"name": EVENT_GATEWAY_NAME, "description": "Multi-region orders demo", "min_runtime_version": "1.2"},
    )
    print(f"Event Gateway created: {EVENT_GATEWAY_NAME} ({created['id']})")
    return created["id"]


def ensure_dp_certificate(gw_id: str) -> tuple[str, str]:
    DP_CERT_DIR.mkdir(parents=True, exist_ok=True)
    cert_path, key_path = DP_CERT_DIR / "tls.crt", DP_CERT_DIR / "key.crt"
    if not (cert_path.exists() and key_path.exists()):
        subprocess.run(
            [
                "openssl", "req", "-new", "-x509", "-nodes", "-newkey", "rsa:2048",
                "-keyout", str(key_path), "-out", str(cert_path),
                "-days", "3650", "-subj", f"/CN={EVENT_GATEWAY_NAME}-dp",
            ],
            check=True, capture_output=True,
        )
        print(f"generated Konnect data-plane client cert: {cert_path}")
        api(
            "POST",
            f"/event-gateways/{gw_id}/data-plane-certificates",
            json={"certificate": cert_path.read_text(), "name": f"{EVENT_GATEWAY_NAME}-dp-cert"},
        )
        print("registered data-plane client cert with Konnect")
    return cert_path.read_text(), key_path.read_text()


def ensure_data_plane_running(gw_id: str) -> None:
    running = subprocess.run(
        ["docker", "ps", "--filter", f"name={DP_CONTAINER_NAME}", "--format", "{{.Names}}"],
        capture_output=True, text=True,
    ).stdout
    if DP_CONTAINER_NAME in running.split():
        print(f"data-plane container already running: {DP_CONTAINER_NAME}")
        return

    cert, key = ensure_dp_certificate(gw_id)
    subprocess.run(["docker", "rm", "-f", DP_CONTAINER_NAME], capture_output=True)
    subprocess.run(
        [
            "docker", "run", "--rm", "-d", "--name", DP_CONTAINER_NAME,
            "-e", "KONG_KONNECT_REGION=" + KONNECT_REGION,
            "-e", "KONG_KONNECT_DOMAIN=konghq.com",
            "-e", f"KONG_KONNECT_GATEWAY_CLUSTER_ID={gw_id}",
            "-e", f"KONG_KONNECT_CLIENT_CERT={cert}",
            "-e", f"KONG_KONNECT_CLIENT_KEY={key}",
            "-p", f"{GATEWAY_PORT}-{GATEWAY_PORT + 9}:{GATEWAY_PORT}-{GATEWAY_PORT + 9}",
            DP_IMAGE,
        ],
        check=True,
    )
    print(f"started data-plane container: {DP_CONTAINER_NAME} (image {DP_IMAGE})")


def ensure_backend_cluster(gw_id: str) -> str:
    bootstrap = os.environ.get("BOOTSTRAP_SERVERS", "").strip()
    api_key = os.environ.get("KAFKA_API_KEY", "").strip()
    api_secret = os.environ.get("KAFKA_API_SECRET", "").strip()
    if not (bootstrap and api_key and api_secret):
        sys.exit(
            "Missing BOOTSTRAP_SERVERS / KAFKA_API_KEY / KAFKA_API_SECRET in .env — "
            "fill these in with your Confluent Cloud cluster's values first."
        )

    existing = find_one(
        api("GET", f"/event-gateways/{gw_id}/backend-clusters")["data"], name="confluent-cloud-orders"
    )
    if existing:
        print(f"backend cluster exists: {existing['id']}")
        return existing["id"]

    body = {
        "name": "confluent-cloud-orders",
        "description": "Confluent Cloud Kafka cluster backing the orders demo",
        "bootstrap_servers": [s.strip() for s in bootstrap.split(",")],
        "authentication": {"type": "sasl_plain", "username": api_key, "password": api_secret},
        "tls": {"enabled": True},
    }
    created = api("POST", f"/event-gateways/{gw_id}/backend-clusters", json=body)
    print(f"backend cluster created: {created['id']}")
    return created["id"]


def ensure_listener(gw_id: str) -> str:
    existing = find_one(api("GET", f"/event-gateways/{gw_id}/listeners")["data"], name="orders-listener")
    if existing:
        print("listener exists: orders-listener")
        return existing["id"]
    created = api(
        "POST",
        f"/event-gateways/{gw_id}/listeners",
        json={"name": "orders-listener", "addresses": ["0.0.0.0"], "ports": [GATEWAY_PORT]},
    )
    print(f"listener created: orders-listener :{GATEWAY_PORT}")
    return created["id"]


def ensure_listener_tls_cert() -> tuple[str, str]:
    CERT_DIR.mkdir(parents=True, exist_ok=True)
    cert_path, key_path = CERT_DIR / "listener-cert.pem", CERT_DIR / "listener-key.pem"
    if not (cert_path.exists() and key_path.exists()):
        subprocess.run(
            [
                "openssl", "req", "-new", "-x509", "-nodes", "-newkey", "rsa:2048",
                "-keyout", str(key_path), "-out", str(cert_path),
                "-days", "3650", "-subj", f"/CN={EVENT_GATEWAY_NAME}",
                "-addext", "subjectAltName=DNS:*.orders.localhost,DNS:*.test.localhost,DNS:*.prod.localhost,DNS:localhost",
            ],
            check=True, capture_output=True,
        )
        print(f"generated self-signed listener TLS cert: {cert_path}")
    return cert_path.read_text(), key_path.read_text()


def ensure_listener_policies(gw_id: str, listener_id: str) -> None:
    existing_types = {p["type"] for p in api("GET", f"/event-gateways/{gw_id}/listeners/{listener_id}/policies")}

    if "tls_server" not in existing_types:
        cert, key = ensure_listener_tls_cert()
        api(
            "POST",
            f"/event-gateways/{gw_id}/listeners/{listener_id}/policies",
            json={
                "type": "tls_server",
                "name": "orders-listener-tls",
                "config": {"allow_plaintext": False, "certificates": [{"certificate": cert, "key": key}]},
            },
        )
        print("listener policy created: tls_server")
    else:
        print("listener policy exists: tls_server")

    if "forward_to_virtual_cluster" not in existing_types:
        api(
            "POST",
            f"/event-gateways/{gw_id}/listeners/{listener_id}/policies",
            json={
                "type": "forward_to_virtual_cluster",
                "name": "orders-listener-sni-routing",
                "config": {"type": "sni", "sni_suffix": ".localhost"},
            },
        )
        print("listener policy created: forward_to_virtual_cluster (sni)")
    else:
        print("listener policy exists: forward_to_virtual_cluster")


def ensure_virtual_cluster(
    gw_id: str, name: str, dns_label: str, backend_id: str, principals: dict, topic_aliases: list | None = None
) -> str:
    existing = find_one(api("GET", f"/event-gateways/{gw_id}/virtual-clusters")["data"], name=name)
    if existing:
        print(f"virtual cluster exists: {name}")
        return existing["id"]

    body = {
        "name": name,
        "dns_label": dns_label,
        "destination": {"id": backend_id},
        "acl_mode": "enforce_on_gateway",
        "authentication": [
            {
                "type": "sasl_plain",
                "mediation": "terminate",
                "principals": [{"username": uname, "password": pwd} for uname, pwd in principals.items()],
            }
        ],
    }
    if topic_aliases:
        body["topic_aliases"] = topic_aliases

    created = api("POST", f"/event-gateways/{gw_id}/virtual-clusters", json=body)
    print(f"virtual cluster created: {name} -> {created['id']}")
    return created["id"]


def acl_rule(resource_type: str, action: str, ops: list, names: list) -> dict:
    return {
        "resource_type": resource_type,
        "action": action,
        "operations": [{"name": o} for o in ops],
        "resource_names": [{"match": n} for n in names],
    }


def ensure_acl_policy(gw_id: str, vc_id: str, name: str, rules: list) -> None:
    existing = find_one(
        api("GET", f"/event-gateways/{gw_id}/virtual-clusters/{vc_id}/cluster-policies"), name=name
    )
    if existing:
        print(f"acl policy exists: {name}")
        return
    api(
        "POST",
        f"/event-gateways/{gw_id}/virtual-clusters/{vc_id}/cluster-policies",
        json={"type": "acls", "name": name, "config": {"rules": rules}},
    )
    print(f"acl policy created: {name}")


def ensure_skip_record_policy(gw_id: str, vc_id: str, name: str, description: str, condition: str) -> None:
    existing = find_one(
        api("GET", f"/event-gateways/{gw_id}/virtual-clusters/{vc_id}/consume-policies"), name=name
    )
    if existing:
        print(f"skip_record policy exists: {name}")
        return
    api(
        "POST",
        f"/event-gateways/{gw_id}/virtual-clusters/{vc_id}/consume-policies",
        json={"type": "skip_record", "name": name, "description": description, "condition": condition},
    )
    print(f"skip_record policy created: {name}")


def main():
    gw_id = get_or_create_event_gateway_id()
    print(f"Event Gateway: {EVENT_GATEWAY_NAME} ({gw_id})")

    ensure_data_plane_running(gw_id)

    backend_id = ensure_backend_cluster(gw_id)
    listener_id = ensure_listener(gw_id)
    ensure_listener_policies(gw_id, listener_id)

    # Passwords aren't returned by the API once set (they're write-only), so
    # if we already generated some in a previous run, keep reusing them —
    # otherwise re-running this script would drift out of sync with the
    # credentials actually configured on the existing virtual clusters.
    principal_names = ["us-producer-svc", "eu-producer-svc", "global-analytics-consumer", "test-consumer", "prod-consumer"]
    existing_config_path = ROOT / "runtime_config.json"
    previous_creds = {}
    if existing_config_path.exists():
        prev = json.loads(existing_config_path.read_text())
        previous_creds = {
            prev["orders_vc"]["producers"]["us"]["username"]: prev["orders_vc"]["producers"]["us"]["password"],
            prev["orders_vc"]["producers"]["eu"]["username"]: prev["orders_vc"]["producers"]["eu"]["password"],
            prev["orders_vc"]["analytics_consumer"]["username"]: prev["orders_vc"]["analytics_consumer"]["password"],
        }
        if "test_vc" in prev:
            previous_creds[prev["test_vc"]["consumer"]["username"]] = prev["test_vc"]["consumer"]["password"]
        if "prod_vc" in prev:
            previous_creds[prev["prod_vc"]["consumer"]["username"]] = prev["prod_vc"]["consumer"]["password"]

    creds = {name: previous_creds.get(name, secrets.token_urlsafe(18)) for name in principal_names}

    orders_vc_id = ensure_virtual_cluster(
        gw_id,
        "orders-vc",
        "orders",
        backend_id,
        {
            "us-producer-svc": creds["us-producer-svc"],
            "eu-producer-svc": creds["eu-producer-svc"],
            "global-analytics-consumer": creds["global-analytics-consumer"],
        },
        topic_aliases=[
            {"alias": "orders", "topic": TOPIC_US, "condition": 'context.auth.principal.name == "us-producer-svc"'},
            {"alias": "orders", "topic": TOPIC_EU, "condition": 'context.auth.principal.name == "eu-producer-svc"'},
        ],
    )

    test_vc_id = ensure_virtual_cluster(
        gw_id, "test-vc", "test", backend_id, {"test-consumer": creds["test-consumer"]}
    )
    prod_vc_id = ensure_virtual_cluster(
        gw_id, "prod-vc", "prod", backend_id, {"prod-consumer": creds["prod-consumer"]}
    )

    ensure_acl_policy(
        gw_id,
        orders_vc_id,
        "orders-vc-acl",
        [
            acl_rule("topic", "allow", ["write", "describe"], ["orders"]),
            acl_rule("topic", "allow", ["read", "describe"], [TOPIC_US, TOPIC_EU]),
            acl_rule("group", "allow", ["read", "describe"], ["*"]),
        ],
    )
    for vc_id, vc_acl_name in [(test_vc_id, "test-vc-acl"), (prod_vc_id, "prod-vc-acl")]:
        ensure_acl_policy(
            gw_id,
            vc_id,
            vc_acl_name,
            [
                acl_rule("topic", "allow", ["read", "describe"], [TOPIC_US, TOPIC_EU]),
                acl_rule("group", "allow", ["read", "describe"], ["*"]),
            ],
        )

    ensure_skip_record_policy(
        gw_id,
        test_vc_id,
        "skip-non-test-records",
        "Test/canary isolation: never deliver a condition=prod record to the test consumer",
        'record.headers["condition"] != "test"',
    )
    ensure_skip_record_policy(
        gw_id,
        prod_vc_id,
        "skip-non-prod-records",
        "Test/canary isolation: never deliver a condition=test record to the prod consumer",
        'record.headers["condition"] != "prod"',
    )

    runtime_config = {
        "gateway_id": gw_id,
        "host": "localhost",
        "topics": {"us": TOPIC_US, "eu": TOPIC_EU},
        "orders_vc": {
            "port": GATEWAY_PORT,
            "dns_label": "orders",
            "producers": {
                "us": {"username": "us-producer-svc", "password": creds["us-producer-svc"]},
                "eu": {"username": "eu-producer-svc", "password": creds["eu-producer-svc"]},
            },
            "analytics_consumer": {
                "username": "global-analytics-consumer",
                "password": creds["global-analytics-consumer"],
            },
        },
        "test_vc": {
            "port": GATEWAY_PORT,
            "dns_label": "test",
            "consumer": {"username": "test-consumer", "password": creds["test-consumer"]},
        },
        "prod_vc": {
            "port": GATEWAY_PORT,
            "dns_label": "prod",
            "consumer": {"username": "prod-consumer", "password": creds["prod-consumer"]},
        },
    }
    (ROOT / "runtime_config.json").write_text(json.dumps(runtime_config, indent=2))
    print(f"\nWrote {ROOT / 'runtime_config.json'}")


if __name__ == "__main__":
    main()
