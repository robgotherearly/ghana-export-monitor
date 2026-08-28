"""Consumer for alerts.flagged - closes the loop back out of Kafka.

Proves the flagged-events topic is a real topic other services can subscribe
to, not just a write-only sink. Logs every alert; optionally forwards to Slack
or Telegram when a webhook/token is configured.

  python -m ingestion.alert_notifier
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys

import requests

from . import config

log = logging.getLogger("alert_notifier")

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
GROUP_ID = os.getenv("ALERT_CONSUMER_GROUP", "gdep-alert-notifier")

_SEVERITY_ICON = {"critical": "🔴", "warning": "🟠", "info": "🔵"}

_running = True


def _stop(signum, _frame) -> None:  # noqa: ANN001
    global _running
    log.info("signal %s received, shutting down", signum)
    _running = False


def format_alert(alert: dict) -> str:
    icon = _SEVERITY_ICON.get(str(alert.get("severity")), "⚪")
    simulated = " [simulated data]" if alert.get("is_simulated") else ""
    return (
        f"{icon} *{str(alert.get('severity', '?')).upper()}* "
        f"{alert.get('entity', '?')} — {alert.get('message', '')}"
        f"{simulated}"
    )


def notify_slack(text: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    try:
        requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10).raise_for_status()
    except requests.RequestException as exc:
        log.warning("slack delivery failed: %s", exc)


def notify_telegram(text: str) -> None:
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        ).raise_for_status()
    except requests.RequestException as exc:
        log.warning("telegram delivery failed: %s", exc)


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )
    from confluent_kafka import Consumer, KafkaError

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _stop)

    consumer = Consumer(
        {
            "bootstrap.servers": config.KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([config.TOPIC_ALERTS])
    log.info("subscribed to %s as %s", config.TOPIC_ALERTS, GROUP_ID)
    if not (SLACK_WEBHOOK_URL or TELEGRAM_BOT_TOKEN):
        log.info("no Slack/Telegram target configured - alerts will only be logged")

    seen = 0
    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("consume error: %s", msg.error())
                continue
            try:
                alert = json.loads(msg.value().decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                log.warning("skipping unparseable alert at offset %s", msg.offset())
                continue

            seen += 1
            text = format_alert(alert)
            log.info("%s", text)
            notify_slack(text)
            notify_telegram(text)
    finally:
        consumer.close()
        log.info("notifier stopped after %d alert(s)", seen)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
