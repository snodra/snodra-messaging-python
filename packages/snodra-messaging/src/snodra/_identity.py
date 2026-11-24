"""Who sent a request: the caller its forwarded token names once verified, the guards a handler puts on its callers,
and what a token must look like without the keys to verify it.

See https://snodra.damoreira.com/docs/library-authors/messages/ ("Forwarded identity").
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Final

from snodra._errors import Unauthenticated
from snodra._jwt import JwksVerifier, json_segment

log = logging.getLogger(__name__)

AUTHORIZATION: Final = "authorization"
"""The request header that carries the caller's token, ``Bearer <JWT>``."""

_SCHEME: Final = "bearer "


@dataclass(frozen=True)
class Caller:
    """The verified caller of the request being handled: the token's subject (``sub``), its roles (read from the claim
    ``Settings.roles_claim`` names) and every claim of the token, read-only."""

    subject: str
    roles: tuple[str, ...]
    claims: Mapping[str, Any]

    def has_role(self, role: str) -> bool:
        """True when the caller has role."""
        return role in self.roles

    def __hash__(self) -> int:
        # The claims are a mapping: a caller hashes by its subject and roles.
        return hash((self.subject, self.roles))


class Guard:
    """Who may call a handler (``guard=`` of ``App.command_handler`` and ``App.query_handler``): made by
    :func:`authenticated`, :func:`role` or :func:`caller_matches`. A request without a verified caller is answered
    ``UNAUTHENTICATED`` before its payload is decoded; a caller the guard's rule refuses, ``PERMISSION_DENIED`` once the
    payload decoded. A guard needs a JWKS (``Settings.jwks_url``): without one, registering it is a
    :class:`ConfigurationError`."""

    __slots__ = ("_describe", "_rule")

    def __init__(self, rule: Callable[[Caller], bool], describe: str) -> None:
        self._rule = rule
        self._describe = describe

    def allows(self, caller: Caller) -> bool:
        """True only when the rule returns ``True`` itself (a truthy value of another type refuses)."""
        return self._rule(caller) is True

    def __repr__(self) -> str:
        return self._describe


def authenticated() -> Guard:
    """Any caller with a verified token."""
    return Guard(lambda _: True, "authenticated()")


def role(name: str) -> Guard:
    """A caller with the role name (see ``Settings.roles_claim``)."""
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"role needs a role name, got {name!r}")
    return Guard(lambda c: c.has_role(name), f"role({name!r})")


def caller_matches(rule: Callable[[Caller], bool]) -> Guard:
    """A caller rule returns ``True`` for; a rule that raises refuses the caller. The rule is a plain function: an
    ``async`` one is a ``TypeError``."""
    if not callable(rule):
        raise TypeError(f"caller_matches needs a function of the caller, got {rule!r}")
    call = rule if inspect.isroutine(rule) else type(rule).__call__  # a function, or an object's __call__
    if inspect.iscoroutinefunction(call):
        raise TypeError(f"caller_matches needs a plain function of the caller, not an async one: {rule!r}")
    name = getattr(rule, "__qualname__", None)
    return Guard(rule, f"caller_matches({name.replace('<locals>.', '') if isinstance(name, str) else repr(rule)})")


def bearer_token(header: str) -> str | None:
    """The token of a ``Bearer <token>`` header value, the scheme in any letter case at its very start; None for a
    value of another shape or a blank token."""
    if header[: len(_SCHEME)].lower() != _SCHEME:
        return None
    return header[len(_SCHEME) :].strip() or None


def check_shape(token: str) -> None:
    """Refuses, without the keys, a token that is not a signed compact JWS (:class:`Unauthenticated`): five segments,
    an encrypted token, ``unsupported token``; not three segments, a header or claims segment that is not base64url
    (padding tolerated) of a JSON object, or an ``alg`` that is not a text, ``unreadable token``; ``alg`` ``none`` in
    any letter case or an empty signature, ``unsigned token``. Nothing of it is read into a caller."""
    parts = token.split(".", 5)
    if len(parts) == 5:
        raise Unauthenticated("unsupported token")
    if len(parts) != 3:
        raise Unauthenticated("unreadable token")
    header = json_segment(parts[0])
    if header is None:
        raise Unauthenticated("unreadable token")
    alg = header.get("alg")
    if alg is not None and not isinstance(alg, str):
        raise Unauthenticated("unreadable token")
    if (isinstance(alg, str) and alg.lower() == "none") or not parts[2]:
        raise Unauthenticated("unsigned token")
    if json_segment(parts[1]) is None:
        raise Unauthenticated("unreadable token")


def read_roles(claims: Mapping[str, Any], claim: str) -> tuple[str, ...]:
    """The roles claim holds in claims: ``scope`` (any letter case) splits the ``scope`` claim at spaces; any other
    value is a dotted path to a list, whose texts are the roles. Empty when it is absent or of another shape."""
    if claim.lower() == "scope":
        scopes = claims.get("scope")
        return _distinct(scopes.split(" ")) if isinstance(scopes, str) else ()
    node: object = claims
    for segment in (s for s in claim.split(".") if s.strip()):
        node = node.get(segment) if isinstance(node, Mapping) else None
    if not isinstance(node, list):
        return ()
    return _distinct(r for r in node if isinstance(r, str))


def _distinct(roles: Any) -> tuple[str, ...]:
    return tuple(dict.fromkeys(r for r in roles if r.strip()))


class IdentityResolver:
    """The caller of a request from its ``authorization`` header: verified against the JWKS when there is one, else
    never an identity."""

    def __init__(self, verifier: JwksVerifier | None, roles_claim: str) -> None:
        self._verifier = verifier
        self._roles_claim = roles_claim
        self._unverified_noted = False

    @property
    def verifies(self) -> bool:
        """True with a JWKS: a token can be an identity."""
        return self._verifier is not None

    async def resolve(self, authorization: str | None) -> Caller | None:
        """The verified caller, or None for an anonymous one (no header, a blank one, or no JWKS to verify the token
        with); :class:`Unauthenticated` for a header that is not ``Bearer <token>`` and a token that fails."""
        if authorization is None or not authorization.strip():
            return None
        token = bearer_token(authorization)
        if token is None:
            raise Unauthenticated("malformed authorization header")
        if self._verifier is None:
            check_shape(token)
            if not self._unverified_noted:
                self._unverified_noted = True
                log.info(
                    "No JWKS (SNODRA_JWKS_URL): forwarded tokens are not verified, so their callers are anonymous "
                    "(the tokens are carried on unread)"
                )
            return None
        claims = await self._verifier.verify(token)
        subject = claims.get("sub")
        return Caller(
            subject if isinstance(subject, str) else "",
            read_roles(claims, self._roles_claim),
            MappingProxyType(claims),
        )

    async def close(self) -> None:
        if self._verifier is not None:
            await self._verifier.close()
