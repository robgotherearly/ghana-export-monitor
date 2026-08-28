"""Provider interface + the fallback chain.

The whole point: free tiers change, get rate limited, or vanish. Nothing in the
pipeline should know or care which upstream answered. A provider is anything
that can say whether it is usable right now and return a list of events.
"""
from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, Generic, TypeVar

import requests

from .. import config

log = logging.getLogger(__name__)

T = TypeVar("T")


class ProviderUnavailable(Exception):
    """Raised when a provider cannot serve this call (no key, quota, upstream 5xx)."""


class Provider(ABC, Generic[T]):
    #: registry name, matches the *_PROVIDERS env chains
    name: str = "unnamed"
    #: human note shown in logs / README
    requires_key: bool = False
    #: politeness floor between two calls to this provider, seconds
    min_interval_s: float = 0.0
    #: hard call budget for the process lifetime (free tiers with monthly caps)
    max_calls: int | None = None

    def __init__(self) -> None:
        self._last_call = 0.0
        self._calls = 0
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "ghana-dep-monitor/1.0 (+portfolio project)"})

    # -- lifecycle -------------------------------------------------------
    def is_configured(self) -> bool:
        """False when a required API key is missing, so the chain skips us."""
        return True

    def is_ready(self) -> bool:
        if not self.is_configured():
            return False
        if self.max_calls is not None and self._calls >= self.max_calls:
            return False
        return (time.time() - self._last_call) >= self.min_interval_s

    def _mark_call(self) -> None:
        self._last_call = time.time()
        self._calls += 1

    # -- http helper -----------------------------------------------------
    def get(self, url: str, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", config.HTTP_TIMEOUT_SECONDS)
        self._mark_call()
        resp = self._session.get(url, **kwargs)
        if resp.status_code == 429:
            raise ProviderUnavailable(f"{self.name}: rate limited (429)")
        if resp.status_code >= 500:
            raise ProviderUnavailable(f"{self.name}: upstream {resp.status_code}")
        resp.raise_for_status()
        return resp

    # -- contract --------------------------------------------------------
    @abstractmethod
    def fetch(self, targets: Sequence[str]) -> list[T]:
        """Return one event per target it could resolve. Partial results are fine."""


class ProviderChain(Generic[T]):
    """Try providers in order; the first that returns anything wins.

    Every fetch reports which provider answered so the payload carries its own
    provenance (`source` field) and the logs show fallbacks as they happen.
    """

    def __init__(self, providers: Sequence[Provider[T]], label: str) -> None:
        self.providers = list(providers)
        self.label = label
        self.stats: dict[str, int] = {p.name: 0 for p in self.providers}

    def fetch(self, targets: Sequence[str]) -> list[T]:
        errors: list[str] = []
        for provider in self.providers:
            if not provider.is_ready():
                continue
            try:
                events = provider.fetch(targets)
            except ProviderUnavailable as exc:
                errors.append(str(exc))
                continue
            except requests.RequestException as exc:
                errors.append(f"{provider.name}: {type(exc).__name__}: {exc}")
                continue
            except Exception as exc:  # a bad parse should demote, not crash the poller
                errors.append(f"{provider.name}: unexpected {type(exc).__name__}: {exc}")
                log.exception("provider %s blew up", provider.name)
                continue
            if events:
                self.stats[provider.name] += 1
                if errors:
                    log.warning("%s: fell through to %s after %s", self.label, provider.name, "; ".join(errors))
                return events
            errors.append(f"{provider.name}: empty response")
        if errors:
            log.error("%s: every provider failed: %s", self.label, "; ".join(errors))
        return []
