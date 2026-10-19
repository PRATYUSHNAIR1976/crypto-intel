"""Shared JSON-over-HTTP GET with retry, rate-limit awareness and raw-page capture."""

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

    Rate-limit awareness, in order of preference:
      1. Proactive: if the provider reports its used request weight in a response header
         (``weight_header``), pause until the next minute once usage passes
         ``weight_threshold`` of ``weight_limit``, before the server starts refusing.
      2. Polite pacing: a fixed ``request_pause_seconds`` between requests.
      3. Reactive: HTTP 429 honours ``Retry-After`` and is retried with backoff.
    5xx/transport errors are retried; other 4xx fail fast. ``fail_fast_messages`` adds a
    provider-specific explanation for particular status codes (e.g. Binance 418 = IP ban).
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
        weight_header: str | None = None,
        weight_limit: int | None = None,
        weight_threshold: float = 0.8,
        clock: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.provider_name = provider_name
        self._client = client or httpx.Client(timeout=timeout)
        self._headers = {"User-Agent": USER_AGENT, **(headers or {})}
        self._pause = request_pause_seconds
        self._fail_fast = fail_fast_messages or {}
        self._weight_header = weight_header
        self._weight_limit = weight_limit
        self._weight_threshold = weight_threshold
        self._clock = clock
        self._sleep = sleep
        self.get = retry_on_exception(
            max_attempts=max_attempts,
            initial_delay=backoff_seconds,
            allowed_exceptions=(RetryableProviderError,),
        )(self._get_once)

    def _log_fields(self, url: str, **extra) -> dict:
        return {"provider": self.provider_name, "url": url, **extra}

    def _get_once(self, url: str, params: dict[str, Any]) -> RawPage:
        started = time.perf_counter()
        try:
            resp = self._client.get(url, params=params, headers=self._headers)
        except httpx.TransportError as exc:
            raise RetryableProviderError(f"{url}: {exc!r}") from exc
        elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
        used_weight = self._used_weight(resp)
        logger.debug("http request", extra=self._log_fields(
            url, event="http_request", status=resp.status_code, elapsed_ms=elapsed_ms, used_weight=used_weight))

        if resp.status_code == 429:
            wait = min(float(resp.headers.get("Retry-After", 1) or 0), 60.0)
            logger.warning("rate limited; backing off", extra=self._log_fields(
                url, event="rate_limited", status=429, retry_after_s=wait))
            self._sleep(wait)
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
        self._throttle_if_near_limit(url, used_weight)
        if self._pause:
            self._sleep(self._pause)
        return page

    def _used_weight(self, resp: httpx.Response) -> int | None:
        if not self._weight_header or self._weight_header not in resp.headers:
            return None
        try:
            return int(resp.headers[self._weight_header])
        except ValueError:
            return None

    def _throttle_if_near_limit(self, url: str, used_weight: int | None) -> None:
        if used_weight is None or not self._weight_limit:
            return
        if used_weight < self._weight_threshold * self._weight_limit:
            return
        now = self._clock()
        wait = 60 - now.second - now.microsecond / 1e6 + 0.5  # weight windows reset each minute
        logger.warning("approaching rate limit; pausing until window resets", extra=self._log_fields(
            url, event="rate_limit_throttle", used_weight=used_weight, weight_limit=self._weight_limit,
            pause_s=round(wait, 2)))
        self._sleep(wait)
