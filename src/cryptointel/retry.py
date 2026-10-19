"""Retry decorator with exponential backoff and jitter.

Adapted from ``stock-index-app/src/retry.py``.
"""

from __future__ import annotations

import logging
import random
import time
from functools import wraps
from typing import Any, Callable

logger = logging.getLogger(__name__)


def retry_on_exception(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    jitter: bool = True,
    allowed_exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable:
    """Retry the wrapped function when it raises one of ``allowed_exceptions``."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception: Exception | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except allowed_exceptions as exc:
                    last_exception = exc
                    if attempt == max_attempts:
                        break
                    delay = initial_delay * (backoff_factor ** (attempt - 1))
                    if jitter and delay > 0:
                        delay += random.uniform(0, 0.5)
                    logger.warning("attempt failed; retrying", extra={
                        "event": "retry", "func": func.__name__, "attempt": attempt,
                        "max_attempts": max_attempts, "delay_s": round(delay, 2), "error": str(exc)})
                    time.sleep(delay)
            logger.error("all attempts failed", extra={
                "event": "retry_exhausted", "func": func.__name__, "max_attempts": max_attempts,
                "error": str(last_exception)})
            raise last_exception  # type: ignore[misc]

        return wrapper

    return decorator
