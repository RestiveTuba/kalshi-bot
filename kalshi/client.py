"""
kalshi/client.py — Async REST client with RSA-PSS authentication.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import time
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import ssl

import aiohttp
import certifi
import structlog
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from config import settings

log = structlog.get_logger(__name__)

_MAX_RETRIES = 4
_BASE_BACKOFF = 0.5


class KalshiClient:
    def __init__(self) -> None:
        self._base_url = settings.base_url
        self._key_id = settings.kalshi_api_key_id
        self._private_key = self._load_key()
        self._session: Optional[aiohttp.ClientSession] = None

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------

    def _load_key(self):
        path = Path(settings.kalshi_private_key_path)
        if not path.exists():
            log.warning("private_key_not_found", path=str(path))
            return None
        pem = path.read_bytes()
        return serialization.load_pem_private_key(pem, password=None)

    def _sign(self, method: str, path: str, body: str = "") -> str:
        ts = str(int(time.time() * 1000))
        msg = ts + method.upper() + path + body
        sig = self._private_key.sign(
            msg.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode(), ts

    def auth_headers(self, method: str, path: str, body: str = "") -> dict[str, str]:
        if not self._private_key:
            return {}
        sig, ts = self._sign(method, path, body)
        return {
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            ssl_ctx = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl=ssl_ctx)
            self._session = aiohttp.ClientSession(
                base_url=self._base_url,
                headers={"Content-Type": "application/json"},
                connector=connector,
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------
    # HTTP helpers with exponential back-off on 429
    # ------------------------------------------------------------------

    async def get(self, path: str, params: Optional[dict] = None) -> dict[str, Any]:
        path = path.lstrip("/")
        qs = "?" + urlencode(params) if params else ""
        full_path = path + qs
        sign_path = "/trade-api/v2/" + path
        return await self._request("GET", full_path, sign_path=sign_path)

    async def post(self, path: str, body: dict) -> dict[str, Any]:
        import json
        path = path.lstrip("/")
        body_str = json.dumps(body)
        sign_path = "/trade-api/v2/" + path
        return await self._request("POST", path, sign_path=sign_path, body_str=body_str, json=body)

    async def _request(
        self, method: str, path: str, *, sign_path: str,
        body_str: str = "", json: Optional[dict] = None
    ) -> dict[str, Any]:
        session = await self._get_session()
        backoff = _BASE_BACKOFF
        last_exc: Exception = RuntimeError("no attempts")

        for attempt in range(_MAX_RETRIES):
            try:
                # Regenerate auth headers on every attempt so the timestamp is fresh
                headers = self.auth_headers(method, sign_path, body_str)
                async with session.request(
                    method, path, headers=headers, json=json
                ) as resp:
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", backoff))
                        log.warning("kalshi_rate_limit", retry_after=retry_after)
                        await asyncio.sleep(retry_after)
                        backoff *= 2
                        continue

                    resp.raise_for_status()
                    return await resp.json()

            except aiohttp.ClientError as exc:
                last_exc = exc
                log.warning(
                    "kalshi_request_error",
                    method=method,
                    path=path,
                    attempt=attempt,
                    error=str(exc),
                )
                await asyncio.sleep(backoff)
                backoff *= 2

        raise last_exc
