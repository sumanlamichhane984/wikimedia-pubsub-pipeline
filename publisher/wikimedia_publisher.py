"""
wikimedia_publisher.py
----------------------
Connects to the Wikimedia real-time edit stream (SSE) and publishes
each event to Google Cloud Pub/Sub.

Routing logic:
  - Valid human edit  → wikimedia-edit-stream
  - Valid bot edit    → wikimedia-bot-stream
  - Broken/invalid    → wikimedia-edit-dlq
"""

from __future__ import annotations

import json
import logging
import time
import unicodedata
from datetime import datetime, timezone
from typing import Any

import requests
from google.cloud import pubsub_v1

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
PROJECT_ID    = "jenish-my-first-dog"
MAIN_TOPIC    = "wikimedia-edit-stream"
BOT_TOPIC     = "wikimedia-bot-stream"
DLQ_TOPIC     = "wikimedia-edit-dlq"

WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# IMPORTANT: Wikimedia requires a fully qualified User-Agent
HEADERS = {
    "User-Agent": (
        "Sabin-Wikimedia-Stream/1.0 "
        "(https://github.com/sabin-wiki-stream; "
        "email: sumanlamichhane984@gmail.com)"
    ),
    "Accept": "text/event-stream",
}

REQUIRED_FIELDS = {"id", "type", "title", "wiki", "timestamp", "user"}

INITIAL_BACKOFF = 2
MAX_BACKOFF     = 60

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pub/Sub client
# ---------------------------------------------------------------------------
batch_settings = pubsub_v1.types.BatchSettings(
    max_messages=100,
    max_bytes=1_000_000,
    max_latency=0.5,
)
publisher_client = pubsub_v1.PublisherClient(batch_settings=batch_settings)

MAIN_TOPIC_PATH = publisher_client.topic_path(PROJECT_ID, MAIN_TOPIC)
BOT_TOPIC_PATH  = publisher_client.topic_path(PROJECT_ID, BOT_TOPIC)
DLQ_TOPIC_PATH  = publisher_client.topic_path(PROJECT_ID, DLQ_TOPIC)

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def normalize_title(title: str) -> str:
    return unicodedata.normalize("NFC", title).strip()


def validate_event(event: dict[str, Any]) -> tuple[bool, str]:
    missing = REQUIRED_FIELDS - event.keys()
    if missing:
        return False, f"Missing fields: {missing}"
    if not isinstance(event.get("title"), str) or not event["title"].strip():
        return False, "title is empty or not a string"
    if not isinstance(event.get("timestamp"), int):
        return False, f"timestamp must be int, got: {type(event.get('timestamp'))}"
    return True, ""


def is_bot(event: dict[str, Any]) -> bool:
    return bool(event.get("bot", False))


def enrich_event(event: dict[str, Any]) -> dict[str, Any]:
    event["normalized_title"] = normalize_title(event.get("title", ""))

    length  = event.get("length") or {}
    old_len = length.get("old")
    new_len = length.get("new")

    event["byte_delta"] = (
        (new_len - old_len)
        if (old_len is not None and new_len is not None)
        else None
    )

    event["is_minor"]     = bool(event.get("minor", False))
    event["user_is_anon"] = event.get("user_id") is None
    event["publisher_ts"] = datetime.now(timezone.utc).isoformat()

    return event


def publish(topic_path: str, payload: dict, attributes: dict) -> None:
    data   = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    future = publisher_client.publish(topic_path, data=data, **attributes)
    future.add_done_callback(_on_done)


def _on_done(future) -> None:
    try:
        future.result()
    except Exception as exc:
        log.error("Publish failed: %s", exc)

# ---------------------------------------------------------------------------
# Main stream loop
# ---------------------------------------------------------------------------

def run():
    backoff  = INITIAL_BACKOFF
    counters = {"total": 0, "human": 0, "bot": 0, "dlq": 0}

    while True:
        log.info("Connecting to Wikimedia stream...")

        try:
            with requests.get(
                WIKIMEDIA_URL,
                stream=True,
                timeout=30,
                headers=HEADERS
            ) as resp:

                resp.raise_for_status()
                backoff = INITIAL_BACKOFF

                log.info("Connected! Listening to Wikipedia edits in real time...")

                for raw_line in resp.iter_lines():
                    if not raw_line:
                        continue
                    if not raw_line.startswith(b"data: "):
                        continue

                    raw_json = raw_line[len(b"data: "):]
                    counters["total"] += 1

                    try:
                        event = json.loads(raw_json)
                    except json.JSONDecodeError as exc:
                        log.warning("Bad JSON received: %s", exc)
                        publish(
                            DLQ_TOPIC_PATH,
                            {"raw": raw_json.decode("utf-8", errors="replace"), "error": str(exc)},
                            {"error_type": "json_parse_error"},
                        )
                        counters["dlq"] += 1
                        continue

                    valid, reason = validate_event(event)
                    if not valid:
                        publish(
                            DLQ_TOPIC_PATH,
                            {"raw": event, "error": reason},
                            {"error_type": "validation_error"},
                        )
                        counters["dlq"] += 1
                        continue

                    event = enrich_event(event)

                    if is_bot(event):
                        publish(
                            BOT_TOPIC_PATH,
                            event,
                            {
                                "wiki": event.get("wiki", ""),
                                "event_type": event.get("type", ""),
                                "traffic": "bot",
                            },
                        )
                        counters["bot"] += 1
                    else:
                        publish(
                            MAIN_TOPIC_PATH,
                            event,
                            {
                                "wiki": event.get("wiki", ""),
                                "event_type": event.get("type", ""),
                                "traffic": "human",
                            },
                        )
                        counters["human"] += 1

                    if counters["total"] % 200 == 0:
                        log.info(
                            "Stats → Total: %d | Human: %d | Bot: %d | DLQ: %d",
                            counters["total"], counters["human"], counters["bot"], counters["dlq"],
                        )

        except requests.exceptions.RequestException as exc:
            log.error("Stream dropped: %s. Reconnecting in %ds...", exc, backoff)
            time.sleep(backoff + (0.5 * time.time() % 1))  # jitter
            backoff = min(backoff * 2, MAX_BACKOFF)


if __name__ == "__main__":
    run()