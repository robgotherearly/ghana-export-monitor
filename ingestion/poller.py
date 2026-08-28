"""The poll loop shared by all three pollers.

Responsibilities kept deliberately small: cadence + jitter, provenance
bookkeeping, graceful shutdown, and handing events to a sink. Everything about
*where the data comes from* lives behind the provider chain.
"""
from __future__ import annotations

import contextlib
import logging
import random
import signal
import time
from collections.abc import Sequence
from dataclasses import dataclass

from . import config
from .kafka_sink import Sink
from .models import Envelope, FxRate, PriceTick
from .providers.base import ProviderChain
from .providers.lastknown import STORE

log = logging.getLogger(__name__)


@dataclass
class PollerSpec:
    label: str
    topic: str
    targets: Sequence[str]
    interval_s: int


class Poller:
    def __init__(self, spec: PollerSpec, chain: ProviderChain, sink: Sink) -> None:
        self.spec = spec
        self.chain = chain
        self.sink = sink
        self._stop = False
        self._cycles = 0
        self._events = 0

    # -- shutdown --------------------------------------------------------
    def install_signal_handlers(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            # not the main thread, or a platform without this signal
            with contextlib.suppress(ValueError, OSError):
                signal.signal(sig, self._handle_signal)

    def _handle_signal(self, signum, _frame) -> None:  # noqa: ANN001
        log.info("%s: signal %s received, finishing current cycle", self.spec.label, signum)
        self._stop = True

    # -- provenance ------------------------------------------------------
    @staticmethod
    def _remember_real_values(events: Sequence[Envelope]) -> None:
        """Feed genuine prints back into the store the simulated fallback walks from."""
        for event in events:
            if isinstance(event, PriceTick) and not event.is_simulated:
                STORE.set(f"commodity:{event.symbol}", event.price)
            elif isinstance(event, FxRate) and not event.is_simulated:
                STORE.set(f"fx:{event.pair}", event.rate)

    # -- loop ------------------------------------------------------------
    def cycle(self) -> int:
        started = time.perf_counter()
        events = self.chain.fetch(self.spec.targets)
        if not events:
            log.warning("%s: cycle produced no events", self.spec.label)
            return 0

        self._remember_real_values(events)
        sent = self.sink.send(self.spec.topic, events)
        self._events += sent
        sources = sorted({getattr(e, "source", "?") for e in events})
        log.info(
            "%s: %d event(s) -> %s via %s in %.0fms",
            self.spec.label,
            sent,
            self.spec.topic,
            ",".join(sources),
            (time.perf_counter() - started) * 1000,
        )
        return sent

    def _sleep(self) -> None:
        jitter = self.spec.interval_s * config.POLL_JITTER_PCT
        delay = max(1.0, self.spec.interval_s + random.uniform(-jitter, jitter))
        deadline = time.time() + delay
        while time.time() < deadline and not self._stop:
            time.sleep(min(0.5, deadline - time.time()))

    def run(self, max_cycles: int | None = None) -> None:
        self.install_signal_handlers()
        log.info(
            "%s: polling %d target(s) every %ss -> %s",
            self.spec.label, len(self.spec.targets), self.spec.interval_s, self.spec.topic,
        )
        while not self._stop:
            try:
                self.cycle()
            except Exception:
                log.exception("%s: cycle failed, continuing", self.spec.label)
            self._cycles += 1
            if max_cycles is not None and self._cycles >= max_cycles:
                break
            if self._stop:
                break
            self._sleep()

        self.sink.close()
        log.info(
            "%s: stopped after %d cycle(s), %d event(s); provider usage=%s",
            self.spec.label, self._cycles, self._events, self.chain.stats,
        )
