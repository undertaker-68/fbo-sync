from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    base_sleep: float


class RateLimiter:
    def __init__(self, rps: float):
        self.rps = max(0.1, float(rps))
        self.min_interval = 1.0 / self.rps
        self._last = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        delta = now - self._last
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self._last = time.monotonic()


class HttpError(RuntimeError):
    def __init__(self, status: int, body: str | None, url: str):
        super().__init__(f"HTTP {status} for {url}: {body[:500] if body else ''}")
        self.status = status
        self.body = body
        self.url = url


class JsonHttpClient:
    def __init__(
        self,
        base_url: str,
        headers: Dict[str, str],
        rps: float,
        retry: RetryPolicy,
        timeout_seconds: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.headers = dict(headers)
        self.timeout_seconds = timeout_seconds
        self.limiter = RateLimiter(rps)
        self.retry = retry
        self.session = requests.Session()

    def request(
        self,
        method: str,
        path: str,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        url = self.base_url + path
        last_exc: Exception | None = None

        for attempt in range(1, self.retry.max_attempts + 1):
            self.limiter.wait()
            try:
                resp = self.session.request(
                    method=method.upper(),
                    url=url,
                    headers=self.headers,
                    params=params,
                    data=json.dumps(json_body) if json_body is not None else None,
                    timeout=self.timeout_seconds,
                )

                if resp.status_code in (429, 500, 502, 503, 504):
                    # retryable
                    body = resp.text
                    raise HttpError(resp.status_code, body, url)

                if resp.status_code < 200 or resp.status_code >= 300:
                    raise HttpError(resp.status_code, resp.text, url)

                if resp.text.strip() == "":
                    return None
                return resp.json()

            except (requests.RequestException, HttpError) as e:
                last_exc = e
                if attempt >= self.retry.max_attempts:
                    raise
                # exponential backoff with jitter
                sleep_s = self.retry.base_sleep * (2 ** (attempt - 1))
                sleep_s = min(sleep_s, 20.0)
                time.sleep(sleep_s + (0.05 * attempt))

        if last_exc:
            raise last_exc
        raise RuntimeError("request failed without exception")
