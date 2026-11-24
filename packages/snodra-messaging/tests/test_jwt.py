"""A forwarded token, judged on its own: the ``Bearer`` header, the shape of a JWS without keys, and RS256 against a
JWKS (signature, ``kid``, ``exp``, ``nbf``, issuer, leeway; the JWKS cache).

See https://snodra.damoreira.com/docs/library-authors/messages/ ("Forwarded identity").
"""

import asyncio
import json
import time
from typing import Any

import pytest
from helpers import ISSUER, Jwks, b64url, eventually, jwk, sign, within
from snodra import Unauthenticated, _jwt
from snodra._identity import bearer_token, check_shape
from snodra._jwt import JwksVerifier


def _verifier(jwks: Jwks, *, fetch_timeout: float = 5.0, min_refresh: float = 60.0) -> JwksVerifier:
    return JwksVerifier(jwks.url, ISSUER, min_refresh=min_refresh, fetch_timeout=fetch_timeout)


def _segment(value: object) -> str:
    return b64url(json.dumps(value).encode())


def test_the_bearer_token_of_an_authorization_header() -> None:
    assert bearer_token("Bearer abc") == "abc"
    assert bearer_token("bearer abc") == "abc"
    assert bearer_token("BEARER  abc ") == "abc"
    for header in ("Basic abc", " Bearer abc", "Bearer", "Bearer ", "Bearer   ", "Bearerabc", "Token abc", ""):
        assert bearer_token(header) is None, header


def test_the_shape_of_a_token_without_keys() -> None:
    signed = f"{_segment({'alg': 'RS256'})}.{_segment({'sub': 'u-1'})}.c2ln"
    check_shape(signed)
    check_shape(f"{_segment({'alg': 'RS256'})}=.{_segment({'sub': 'u-1'})}.c2ln")  # padding tolerated
    check_shape(f"{_segment({'typ': 'JWT'})}.{_segment({})}.c2ln")  # no alg: not judged here
    cases = {
        "a.b.c.d.e": "unsupported token",
        "a.b": "unreadable token",
        "abc": "unreadable token",
        "a.b.c.d": "unreadable token",
        "a.b.c.d.e.f": "unreadable token",
        f"not-json.{_segment({})}.c2ln": "unreadable token",
        f"{b64url(b'[1]')}.{_segment({})}.c2ln": "unreadable token",
        f"{_segment({'alg': 5})}.{_segment({})}.c2ln": "unreadable token",
        f"{_segment({'alg': 'RS256'})}.{b64url(b'nope')}.c2ln": "unreadable token",
        f"{_segment({'alg': 'RS256'})}.{_segment({})[:-1]}!.c2ln": "unreadable token",
        f"{_segment({'alg': 'none'})}.{_segment({})}.c2ln": "unsigned token",
        f"{_segment({'alg': 'NONE'})}.{_segment({})}.c2ln": "unsigned token",
        f"{_segment({'alg': 'RS256'})}.{_segment({})}.": "unsigned token",
    }
    for token, text in cases.items():
        with pytest.raises(Unauthenticated, match=f"^{text}$"):
            check_shape(token)


async def test_a_valid_token_gives_its_claims(jwks: Jwks) -> None:
    v = _verifier(jwks)
    claims = await v.verify(jwks.mint(claims={"nbf": int(time.time())}))
    assert (claims["sub"], claims["roles"], claims["iss"]) == ("u-1", ["orders-user"], ISSUER)
    await v.close()


async def test_the_tokens_a_verifier_refuses(jwks: Jwks) -> None:
    jwks.published.append({"kty": "EC", "kid": "k-ec", "crv": "P-256", "x": "AA", "y": "AA"})
    v = _verifier(jwks)
    now = time.time()
    header = {"alg": "RS256", "kid": "k-1"}
    cases = {
        "abc": "unreadable token",
        "a.b.c.d.e": "unreadable token",
        f"not-json.{_segment({})}.c2ln": "unreadable token",
        sign({"alg": "RS512", "kid": "k-1"}, {"iss": ISSUER, "exp": now + 60}, jwks.key): "unsupported token algorithm",
        f"{_segment({'alg': 'none', 'kid': 'k-1'})}.{_segment({'iss': ISSUER, 'exp': now + 60})}.": (
            "unsupported token algorithm"
        ),
        sign({"alg": "RS256"}, {"iss": ISSUER, "exp": now + 60}, jwks.key): "token has no kid",
        sign(header | {"crit": ["exp"]}, {"iss": ISSUER, "exp": now + 60}, jwks.key): "unsupported critical header",
        jwks.mint(key=jwks.other): "invalid token",
        jwks.mint()[:-4] + "AAAA": "invalid token",
        jwks.mint() + "!": "invalid token",
        jwks.mint(kid="k-ec"): "unusable signing key",
        jwks.mint(exp=now - 61): "token expired",
        jwks.mint(issuer="https://other.test"): "token issuer not accepted",
        jwks.mint(claims={"nbf": now + 61}): "token not valid yet",
        jwks.mint(claims={"nbf": "soon"}): "invalid token",
        jwks.mint(claims={"exp": "later"}): "token has no expiry",
        jwks.mint(claims={"exp": True}): "token has no expiry",
        sign(header, {"iss": ISSUER, "sub": "u-1"}, jwks.key): "token has no expiry",
        sign(header, ["not", "an", "object"], jwks.key): "invalid token",  # type: ignore[arg-type]
    }
    for token, text in cases.items():
        with pytest.raises(Unauthenticated, match=f"^{text}$"):
            await v.verify(token)
    await v.close()


async def test_the_leeway_is_sixty_seconds(jwks: Jwks) -> None:
    v = _verifier(jwks)
    now = time.time()
    await v.verify(jwks.mint(exp=now - 50))
    await v.verify(jwks.mint(claims={"nbf": now + 50}))
    await v.close()


async def test_the_first_key_of_a_repeated_kid_wins(jwks: Jwks) -> None:
    jwks.published.append(jwk(jwks.other, "k-1"))
    v = _verifier(jwks)
    await v.verify(jwks.mint())
    with pytest.raises(Unauthenticated, match=r"^invalid token$"):
        await v.verify(jwks.mint(key=jwks.other))
    await v.close()


async def test_the_keys_are_fetched_once_and_cached(jwks: Jwks) -> None:
    v = _verifier(jwks)
    await asyncio.gather(*(v.verify(jwks.mint()) for _ in range(5)))  # concurrent tokens share one fetch
    await v.verify(jwks.mint())
    assert jwks.fetches == 1
    await v.close()


async def test_a_failed_first_load_is_tried_again_at_most_once_per_minimum_refresh(jwks: Jwks) -> None:
    jwks.down = True
    v = _verifier(jwks, min_refresh=0.3)
    with pytest.raises(Unauthenticated, match=r"^signing keys unavailable$"):
        await v.verify(jwks.mint())
    failed = time.monotonic()
    jwks.down = False
    with pytest.raises(Unauthenticated, match=r"^signing keys unavailable$"):
        await v.verify(jwks.mint())  # within the minimum refresh: refused without a fetch
    assert jwks.fetches == 1
    while True:  # then tried again, until it succeeds
        try:
            await v.verify(jwks.mint())
            break
        except Unauthenticated:
            assert time.monotonic() - failed < 2.0, "the keys were never fetched again"
            await asyncio.sleep(0.02)
    assert time.monotonic() - failed >= 0.3
    assert jwks.fetches == 2
    await v.close()


async def _refused_until(v: JwksVerifier, jwks: Jwks, fetches: int, within: float = 3.0) -> float:
    """Verifies tokens (each refused) until the JWKS was fetched ``fetches`` times; when that happened."""
    deadline = time.monotonic() + within
    while jwks.fetches < fetches:
        with pytest.raises(Unauthenticated, match=r"^signing keys unavailable$"):
            await v.verify(jwks.mint())
        assert time.monotonic() < deadline, f"fetched {jwks.fetches} times, never {fetches}"
        await asyncio.sleep(0.01)
    return time.monotonic()


async def test_a_failed_first_load_is_retried_after_a_doubling_wait(jwks: Jwks) -> None:
    jwks.down = True
    v = JwksVerifier(jwks.url, ISSUER, min_refresh=60.0, fetch_timeout=5.0, first_retry=0.1)
    first = await _refused_until(v, jwks, 1)
    second = await _refused_until(v, jwks, 2)  # not a minute later: 0.1 s after the failure
    third = await _refused_until(v, jwks, 3)
    within(second - first, 0.09, 0.5)
    within(third - second, 0.19, 0.6)  # doubled
    jwks.down = False
    while True:  # doubled again, then loaded
        try:
            claims = await v.verify(jwks.mint())
            break
        except Unauthenticated:
            assert time.monotonic() - third < 1.5, "the keys were never fetched again"
            await asyncio.sleep(0.01)
    assert claims["sub"] == "u-1"
    assert time.monotonic() - third >= 0.39
    assert jwks.fetches == 4
    # Once the keys are loaded, the minimum refresh is the minute again: an unknown kid fetches nothing more.
    with pytest.raises(Unauthenticated, match=r"^unknown signing key$"):
        await v.verify(jwks.mint(kid="k-2"))
    assert jwks.fetches == 4
    await v.close()


def test_the_first_retry_is_at_most_the_minimum_refresh(jwks: Jwks) -> None:
    v = JwksVerifier(jwks.url, ISSUER, min_refresh=0.3, fetch_timeout=5.0)
    assert v.first_retry == 0.3  # 1 s by default, never above the minimum refresh


async def test_an_unknown_kid_right_after_the_first_load_fetches_once(jwks: Jwks) -> None:
    v = _verifier(jwks)
    with pytest.raises(Unauthenticated, match=r"^unknown signing key$"):
        await v.verify(jwks.mint(kid="k-2"))
    assert jwks.fetches == 1  # the first load counts as the refresh
    await v.close()


async def test_a_redirect_is_never_followed(jwks: Jwks) -> None:
    for location in ("/elsewhere", "ftp://127.0.0.1/jwks", "https://issuer.test/jwks"):
        jwks.redirect = location
        v = _verifier(jwks)
        with pytest.raises(Unauthenticated, match=r"^signing keys unavailable$"):
            await v.verify(jwks.mint())
        await v.close()
    assert jwks.paths == ["/jwks"] * 3


async def test_the_jwks_is_parsed_off_the_event_loop(jwks: Jwks, monkeypatch: pytest.MonkeyPatch) -> None:
    on_loop: list[bool] = []
    parse = _jwt.parse_jwks

    def watched(body: bytes) -> Any:
        try:
            asyncio.get_running_loop()
            on_loop.append(True)
        except RuntimeError:
            on_loop.append(False)
        return parse(body)

    monkeypatch.setattr(_jwt, "parse_jwks", watched)
    v = _verifier(jwks)
    await v.verify(jwks.mint())
    assert on_loop == [False]
    await v.close()


async def test_the_fetch_timeout_bounds_the_whole_download(jwks: Jwks) -> None:
    jwks.dribble = True  # every byte in time for a per-read timeout, the whole far beyond the fetch timeout
    v = _verifier(jwks, fetch_timeout=0.2)
    with pytest.raises(Unauthenticated, match=r"^signing keys unavailable$"):
        await v.verify(jwks.mint())
    await eventually(lambda: jwks.dribble_cut, within=1.0)  # the download itself was cut, not only the wait
    await v.close()


async def test_a_jwks_larger_than_one_mebibyte_is_refused(jwks: Jwks) -> None:
    jwks.published.append({"kty": "oct", "kid": "big", "k": "A" * (1 << 20)})
    v = _verifier(jwks)
    with pytest.raises(Unauthenticated, match=r"^signing keys unavailable$"):
        await v.verify(jwks.mint())
    await v.close()


async def test_a_slow_jwks_is_bounded_by_the_fetch_timeout(jwks: Jwks) -> None:
    jwks.delay = 1.0
    v = _verifier(jwks, fetch_timeout=0.2)
    started = time.monotonic()
    with pytest.raises(Unauthenticated, match=r"^signing keys unavailable$"):
        await v.verify(jwks.mint())
    assert time.monotonic() - started < 0.8
    await v.close()


async def test_a_waiter_cancelled_leaves_the_fetch_to_the_others(jwks: Jwks) -> None:
    jwks.delay = 0.3
    v = _verifier(jwks)
    first = asyncio.create_task(v.verify(jwks.mint()))
    second = asyncio.create_task(v.verify(jwks.mint()))
    await eventually(lambda: jwks.fetches == 1)  # both wait on the fetch
    second.cancel()
    with pytest.raises(asyncio.CancelledError):
        await second
    assert (await first)["sub"] == "u-1"
    assert jwks.fetches == 1
    await v.close()


async def test_a_closed_verifier_ends_its_fetch_and_refuses(jwks: Jwks) -> None:
    jwks.delay = 1.0
    v = _verifier(jwks)
    waiting = asyncio.create_task(v.verify(jwks.mint()))
    await eventually(lambda: jwks.fetches == 1)
    await v.close()
    with pytest.raises(Unauthenticated, match=r"^signing keys unavailable$"):
        await waiting
    assert not [t for t in asyncio.all_tasks() if t.get_name().startswith("snodra:") and not t.done()]
    with pytest.raises(Unauthenticated, match=r"^signing keys unavailable$"):
        await v.verify(jwks.mint())
