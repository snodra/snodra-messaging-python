"""A forwarded token verified against the issuer's signing keys: RS256 over a JWKS, the issuer, the expiry and the
not-before, and the cache of the keys.

See https://snodra.damoreira.com/docs/library-authors/messages/ ("Forwarded identity").
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import math
import time
import urllib.request
from typing import Any, Final

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from snodra._errors import Unauthenticated
from snodra._meta import client_version

log = logging.getLogger(__name__)

MAX_JWKS_BYTES: Final = 1 << 20
"""The largest JWKS document read."""

UNAVAILABLE: Final = "signing keys unavailable"
"""Why a token is refused when the JWKS cannot be fetched."""

_MIN_RSA_BITS: Final = 2048


def b64url_decode(segment: str) -> bytes:
    """A base64url segment's bytes (padding tolerated); ``ValueError`` for anything else."""
    s = segment.rstrip("=")
    if len(s) % 4 == 1 or "+" in s or "/" in s:
        raise ValueError("not base64url")
    return base64.b64decode(s + "=" * (-len(s) % 4), altchars=b"-_", validate=True)


def json_segment(segment: str) -> dict[str, Any] | None:
    """The JSON object a base64url segment (padding tolerated) holds, or None when it holds anything else."""
    try:
        text = b64url_decode(segment).decode("utf-8").strip()
        if not text.startswith("{"):
            return None
        value = json.loads(text)
    except (ValueError, RecursionError):
        return None
    return value if isinstance(value, dict) else None


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
        return None
    return float(value)


class JwksVerifier:
    """Verifies RS256 tokens against the keys of the JWKS at ``url``, and their issuer, expiry and not-before (each
    with ``leeway`` seconds of clock skew).

    The keys are fetched at the first token and cached; until a fetch succeeds every token that needs them fetches
    again. A token whose ``kid`` the cache does not know fetches them again, at most once per ``min_refresh``. A fetch
    runs off the event loop (``urllib`` in a thread, at most ``fetch_timeout`` seconds and 1 MiB) in one task,
    ``snodra:jwks``, that every token needing the keys meanwhile waits for: a waiter that is cancelled leaves it to
    the others. A token is refused with :class:`Unauthenticated` and a short reason, safe to send back to the caller.
    """

    def __init__(
        self,
        url: str,
        issuer: str,
        *,
        min_refresh: float,
        fetch_timeout: float,
        leeway: float = 60.0,
        max_bytes: int = MAX_JWKS_BYTES,
    ) -> None:
        self._url = url
        self._issuer = issuer
        self._min_refresh = min_refresh
        self._fetch_timeout = fetch_timeout
        self._leeway = leeway
        self._max_bytes = max_bytes
        self._keys: dict[str, rsa.RSAPublicKey | None] | None = None
        """The published keys by id (None: published but unusable); None until a fetch succeeded."""
        self._last_forced: float | None = None
        self._fetch: asyncio.Task[dict[str, rsa.RSAPublicKey | None]] | None = None
        self._closed = False

    async def verify(self, token: str) -> dict[str, Any]:
        """The claims of token once its signature, issuer, expiry and not-before hold; :class:`Unauthenticated`
        otherwise (``signing keys unavailable`` when the JWKS cannot be fetched)."""
        parts = token.split(".", 5)  # at most 6: a token of many dots costs nothing
        if len(parts) != 3:
            raise Unauthenticated("unreadable token")
        header = json_segment(parts[0])
        if header is None:
            raise Unauthenticated("unreadable token")
        if header.get("alg") != "RS256":
            raise Unauthenticated("unsupported token algorithm")
        kid = header.get("kid")
        if not isinstance(kid, str) or not kid:
            raise Unauthenticated("token has no kid")
        key = await self._key(kid)
        try:
            signature = b64url_decode(parts[2])
            key.verify(signature, f"{parts[0]}.{parts[1]}".encode("ascii"), padding.PKCS1v15(), hashes.SHA256())
        except (ValueError, InvalidSignature):
            raise Unauthenticated("invalid token") from None
        claims = json_segment(parts[1])
        if claims is None:
            raise Unauthenticated("invalid token")
        self._check(claims, time.time())
        return claims

    async def close(self) -> None:
        """Ends the fetch in progress, if any; every token is refused from now on."""
        self._closed = True
        task = self._fetch
        if task is not None and not task.done():
            task.cancel()
            await asyncio.wait({task})

    def _check(self, claims: dict[str, Any], now: float) -> None:
        exp = _number(claims.get("exp"))
        if exp is None:
            raise Unauthenticated("token has no expiry")
        if exp + self._leeway < now:
            raise Unauthenticated("token expired")
        if "nbf" in claims:
            nbf = _number(claims["nbf"])
            if nbf is None:
                raise Unauthenticated("invalid token")
            if nbf - self._leeway > now:
                raise Unauthenticated("token not valid yet")
        if claims.get("iss") != self._issuer:
            raise Unauthenticated("token issuer not accepted")

    async def _key(self, kid: str) -> rsa.RSAPublicKey:
        if self._keys is None:
            await self._load(forced=False)
        if self._keys is None or kid not in self._keys:
            await self._load(forced=True)
        keys = self._keys or {}
        if kid not in keys:
            raise Unauthenticated("unknown signing key")
        key = keys[kid]
        if key is None:
            raise Unauthenticated("unusable signing key")
        return key

    async def _load(self, *, forced: bool) -> None:
        """Waits for a fetch of the keys: the one in progress, else a new one. A forced fetch (an unknown ``kid``)
        starts at most once per ``min_refresh``; within it, nothing is fetched."""
        if self._closed:
            raise Unauthenticated(UNAVAILABLE)
        task = self._fetch
        if task is None:
            now = asyncio.get_running_loop().time()
            if forced:
                if self._last_forced is not None and now - self._last_forced < self._min_refresh:
                    return
                self._last_forced = now
            task = asyncio.create_task(self._fetch_keys(), name="snodra:jwks")
            self._fetch = task
            task.add_done_callback(self._fetched)
        try:
            await asyncio.shield(task)  # a waiter's cancellation never cancels the fetch the others wait for
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if task.cancelled() and (current is None or not current.cancelling()):
                raise Unauthenticated(UNAVAILABLE) from None  # the verifier was closed
            raise
        except Exception:
            raise Unauthenticated(UNAVAILABLE) from None

    def _fetched(self, task: asyncio.Task[dict[str, rsa.RSAPublicKey | None]]) -> None:
        if self._fetch is task:
            self._fetch = None
        if task.cancelled():
            return
        if (e := task.exception()) is not None:
            log.warning("The JWKS at %s could not be fetched: tokens are refused (%s)", self._url, e)
            return
        self._keys = task.result()

    async def _fetch_keys(self) -> dict[str, rsa.RSAPublicKey | None]:
        async with asyncio.timeout(self._fetch_timeout):
            body = await asyncio.to_thread(self._download)
        return parse_jwks(body)

    def _download(self) -> bytes:
        request = urllib.request.Request(
            self._url, headers={"Accept": "application/json", "User-Agent": client_version()}
        )
        with urllib.request.urlopen(
            request, timeout=self._fetch_timeout
        ) as response:  # http:// or https:// only (Settings)
            body: bytes = response.read(self._max_bytes + 1)
        if len(body) > self._max_bytes:
            raise ValueError(f"the JWKS is larger than {self._max_bytes} bytes")
        return body


def parse_jwks(body: bytes) -> dict[str, rsa.RSAPublicKey | None]:
    """The keys of a JWKS document by ``kid``, the first of a repeated id winning; None for a key that is not a usable
    RSA key. ``ValueError`` when the document is not a JWKS."""
    doc = json.loads(body)
    if not isinstance(doc, dict) or not isinstance(doc.get("keys"), list):
        raise ValueError("the JWKS has no keys list")
    keys: dict[str, rsa.RSAPublicKey | None] = {}
    for entry in doc["keys"]:
        if not isinstance(entry, dict):
            continue
        kid = entry.get("kid")
        if not isinstance(kid, str) or kid in keys:
            continue
        keys[kid] = _rsa_key(entry)
    return keys


def _rsa_key(entry: dict[str, Any]) -> rsa.RSAPublicKey | None:
    """The RSA public key a JWK describes, None when it is not one this library verifies with."""
    if entry.get("kty") != "RSA" or entry.get("use", "sig") != "sig" or entry.get("alg", "RS256") != "RS256":
        return None
    n_text, e_text = entry.get("n"), entry.get("e")
    if not isinstance(n_text, str) or not isinstance(e_text, str):
        return None
    try:
        n = int.from_bytes(b64url_decode(n_text), "big")
        e_bytes = b64url_decode(e_text)
        if not 0 < len(e_bytes) <= 4:
            return None
        e = int.from_bytes(e_bytes, "big")
        if n.bit_length() < _MIN_RSA_BITS:
            return None
        return rsa.RSAPublicNumbers(e, n).public_key()
    except ValueError:
        return None
