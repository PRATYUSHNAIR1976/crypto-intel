"""Shared JSON-over-HTTP GET with retry, rate-limit handling and raw-page capture."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Callable

import httpx

from cryptointel.domain.models import utc_now
from cryptointel.providers.base import ProviderError, RawPage, RetryableProviderError
from cryptointel.retry import retry_on_exception

logger = logging.getLogger(__name__)

USER_AGENT = "crypto-intel/0.1 (local research)"


class JsonHttpClient:
    """GET JSON from a public endpoint.

    429 and 5xx/transport errors are retried with backoff (429 honours Retry-After);
    any other 4xx fails fast. ``fail_fast_messages`` adds a provider-specific
    explanation for particular status codes (e.g. Binance 418 = IP ban).
    """

    def __init__(
        self,
        provider_name: str,
        client: httpx.Client | None = None,
        timeout: float = 15.0,
        headers: dict[str, str] | None = None,
        request_pause_seconds: float = 0.0,
        max_attempts: int = 4,
        backoff_seconds: float = 1.0,
        fail_fast_messages: dict[int, str] | None = None,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self.provider_name = provider_name
        self._client = client or httpx.Client(timeout=timeout)
        self._headers = {"User-Agent": USER_AGENT, **(headers or {})}
        self._pause = request_pause_seconds
        self._fail_fast = fail_fast_messages or {}
        self._clock = clock
        self.get = retry_on_exception(
            max_attempts=max_attempts,
            initial_delay=backoff_seconds,
            allowed_exceptions=(RetryableProviderError,),
        )(self._get_once)

    def _get_once(self, url: str, params: dict[str, Any]) -> RawPage:
        try:
            resp = self._client.get(url, params=params, headers=self._headers)
        except httpx.TransportError as exc:
            raise RetryableProviderError(f"{url}: {exc!r}") from exc

        if resp.status_code == 429:
            wait = min(float(resp.headers.get("Retry-After", 1) or 0), 60.0)
            logger.warning("Rate limited by %s; sleeping %.1fs", self.provider_name, wait)
            time.sleep(wait)
            raise RetryableProviderError(f"{url}: HTTP 429 rate limited")
        if resp.status_code >= 500:
            raise RetryableProviderError(f"{url}: HTTP {resp.status_code}")
        if resp.status_code in self._fail_fast:
            raise ProviderError(f"{self.provider_name} returned {resp.status_code}: {self._fail_fast[resp.status_code]}")
        if resp.status_code >= 400:
            raise ProviderError(f"{url} {params} -> HTTP {resp.status_code}: {resp.text[:300]}")

        page = RawPage(
            url=url, params=dict(params), status_code=resp.status_code,
            fetched_at=self._clock(), payload=resp.json(),
        )
        if self._pause:
            time.sleep(self._pause)
        return page
