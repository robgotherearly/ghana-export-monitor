"""Entry point for all three pollers: python -m ingestion.run_poller --source fx"""
from __future__ import annotations

import argparse
import logging
import os
import sys

from . import config
from .kafka_sink import build_sink
from .poller import Poller, PollerSpec
from .providers import registry


def _configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )


def build(source: str) -> tuple[PollerSpec, object]:
    if source == "commodities":
        return (
            PollerSpec("commodities", config.TOPIC_COMMODITY_RAW,
                       config.ACTIVE_COMMODITIES, config.COMMODITY_POLL_SECONDS),
            registry.commodity_chain(),
        )
    if source == "fx":
        return (
            PollerSpec("fx", config.TOPIC_FX_RAW,
                       config.ACTIVE_FX_PAIRS, config.FX_POLL_SECONDS),
            registry.fx_chain(),
        )
    if source == "weather":
        return (
            PollerSpec("weather", config.TOPIC_WEATHER_RAW,
                       config.ACTIVE_REGIONS, config.WEATHER_POLL_SECONDS),
            registry.weather_chain(),
        )
    raise SystemExit(f"unknown source {source!r}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ghana DEP ingestion poller")
    parser.add_argument("--source", required=True, choices=["commodities", "fx", "weather"])
    parser.add_argument("--once", action="store_true", help="run a single cycle and exit")
    parser.add_argument("--cycles", type=int, default=None, help="stop after N cycles")
    parser.add_argument("--dry-run", action="store_true", help="print to stdout instead of Kafka")
    args = parser.parse_args(argv)

    _configure_logging()
    spec, chain = build(args.source)
    sink = build_sink(dry_run=args.dry_run, client_id=f"gdep-poller-{args.source}")
    poller = Poller(spec, chain, sink)  # type: ignore[arg-type]
    poller.run(max_cycles=1 if args.once else args.cycles)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
