"""Shared helpers for the event gateway demo scripts."""
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RUNTIME_CONFIG_PATH = ROOT / "runtime_config.json"


def load_runtime_config() -> dict:
    if not RUNTIME_CONFIG_PATH.exists():
        raise SystemExit(
            f"{RUNTIME_CONFIG_PATH} not found — run scripts/setup_konnect.py first."
        )
    return json.loads(RUNTIME_CONFIG_PATH.read_text())


def save_runtime_config(config: dict) -> None:
    RUNTIME_CONFIG_PATH.write_text(json.dumps(config, indent=2))


def dashboard_url() -> str:
    return os.environ.get("DASHBOARD_URL", "http://127.0.0.1:8090")
