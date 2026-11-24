"""TLS to the hubs and the client certificate (mutual TLS): the operator's CA and the name to verify, the certificate
files checked at start, read at every connect and every few seconds, and a renewed pair rolled over to before the
presented one ends, one hub at a time.

See https://snodra.damoreira.com/docs/library-authors/protocol/ ("Credentials").
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import re
import threading
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Final

import grpc
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, ed25519, rsa

from snodra._errors import ConfigurationError
from snodra._timings import Timings

if TYPE_CHECKING:
    from snodra._connection import HubConnection

log = logging.getLogger(__name__)

SERVICE_SAN_PREFIX: Final = "snodra://service/"
"""The URI SAN that names the service of a client certificate, as the hub reads it."""

_SERVICE_NAME: Final = re.compile(r"[A-Za-z0-9._-]{1,128}")
_CERT: Final = "client_certificate.cert_file (SNODRA_SERVER_TLS_CLIENT_CERT)"
_KEY: Final = "client_certificate.key_file (SNODRA_SERVER_TLS_CLIENT_KEY)"


@dataclass(frozen=True)
class TLSConfig:
    """TLS to every hub (``SNODRA_SERVER_TLS``): a hub with TLS on accepts nothing else. ``ca_file``
    (``SNODRA_SERVER_TLS_CA``) is a PEM file of the CA certificates that sign the hubs' certificates, read again at
    every connect (None: grpcio's bundled roots); ``server_name`` (``SNODRA_SERVER_TLS_SERVER_NAME``) is the name the
    hubs' certificates must carry (None: the host of each hub address)."""

    ca_file: str | None = None
    server_name: str | None = None


@dataclass(frozen=True)
class ClientCertificate:
    """The client certificate the instance presents to every hub (mutual TLS; ``SNODRA_SERVER_TLS_CLIENT_CERT``,
    ``SNODRA_SERVER_TLS_CLIENT_KEY``): the PEM chain, leaf first, and its private key. It implies TLS. The leaf must
    carry exactly one URI SAN ``snodra://service/<name>`` naming the service: the pair is checked when the app is built
    (:class:`ConfigurationError`). The files are read at every connect and every 3 s: a renewed, valid pair is rolled
    over to before the presented certificate ends, after a random delay of at most a tenth of its remaining validity
    (at most 30 s), one hub at a time; a pair that cannot be used keeps the one presented."""

    cert_file: str
    key_file: str


class CertificateProblem(Exception):
    """A client certificate pair that cannot be used; the message names the files, never their content."""


@dataclass(frozen=True, repr=False)
class Pair:
    """A client certificate pair that was read and checked."""

    chain_pem: bytes
    key_pem: bytes
    service: str
    fingerprint: str
    """The first 8 hex characters of the SHA-256 of the leaf: the only form of the certificate that is logged."""
    serial: int
    not_before: float
    not_after: float
    raw: tuple[bytes, bytes] = field(compare=False)

    def __repr__(self) -> str:
        return f"Pair(service={self.service!r}, fingerprint={self.fingerprint!r}, not_after={_when(self.not_after)})"


def _when(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, UTC).isoformat(timespec="seconds")


def roll_delay_bound(maximum: float, remaining: float) -> float:
    """The bound of the random delay before a roll: ``maximum``, or a tenth of what the presented certificate has
    left, whichever is smaller; zero once it has ended."""
    # TODO(spec-gap): SG-4
    return max(0.0, min(maximum, remaining / 10))


def _read(path: str, setting: str) -> bytes:
    try:
        data = Path(path).read_bytes()
    except OSError as e:
        raise CertificateProblem(f"{setting} {path} cannot be read: {type(e).__name__}") from None
    if not data.strip():
        raise CertificateProblem(f"{setting} {path} is empty")
    return data


def read_files(files: ClientCertificate) -> tuple[bytes, bytes]:
    return _read(files.cert_file, _CERT), _read(files.key_file, _KEY)


def parse_pair(files: ClientCertificate, raw: tuple[bytes, bytes], service: str) -> Pair:
    """The pair of the files' bytes for service; :class:`CertificateProblem` when it cannot be used. Its validity
    period is not judged here."""
    cert_pem, key_pem = raw
    try:
        chain = x509.load_pem_x509_certificates(cert_pem)
    except ValueError:
        raise CertificateProblem(f"{_CERT} {files.cert_file} is not a PEM certificate chain") from None
    if not chain:
        raise CertificateProblem(f"{_CERT} {files.cert_file} holds no PEM certificate")
    leaf = chain[0]
    try:
        key = serialization.load_pem_private_key(key_pem, password=None)
    except (ValueError, TypeError):
        raise CertificateProblem(
            f"{_KEY} {files.key_file} is not an unencrypted PEM private key (RSA, EC or Ed25519)"
        ) from None
    if not isinstance(key, rsa.RSAPrivateKey | ec.EllipticCurvePrivateKey | ed25519.Ed25519PrivateKey):
        raise CertificateProblem(f"{_KEY} {files.key_file} is not an RSA, EC or Ed25519 private key")
    spki = serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    if key.public_key().public_bytes(*spki) != leaf.public_key().public_bytes(*spki):
        raise CertificateProblem(
            f"{_KEY} {files.key_file} does not match the first certificate of {_CERT} {files.cert_file} "
            "(the leaf goes first)"
        )
    name = _service_of(files, leaf)
    if name != service:
        raise CertificateProblem(
            f"{_CERT} {files.cert_file} names the service {name} but the service is {service} (SNODRA_SERVICE_NAME): "
            "the hub refuses a registration under a name the certificate does not carry"
        )
    return Pair(
        chain_pem=cert_pem,
        key_pem=key_pem,
        service=name,
        fingerprint=hashlib.sha256(leaf.public_bytes(serialization.Encoding.DER)).hexdigest()[:8],
        serial=leaf.serial_number,
        not_before=leaf.not_valid_before_utc.timestamp(),
        not_after=leaf.not_valid_after_utc.timestamp(),
        raw=raw,
    )


def _service_of(files: ClientCertificate, leaf: x509.Certificate) -> str:
    """The one ``snodra://service/<name>`` URI SAN of leaf, as the hub reads it; other SANs are ignored."""
    try:
        sans = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        uris = sans.get_values_for_type(x509.UniformResourceIdentifier)
    except x509.ExtensionNotFound:
        uris = []
    except ValueError:
        raise CertificateProblem(f"{_CERT} {files.cert_file} has unreadable subject alternative names") from None
    named = [u.removeprefix(SERVICE_SAN_PREFIX) for u in uris if u.startswith(SERVICE_SAN_PREFIX)]
    if not named:
        raise CertificateProblem(
            f"{_CERT} {files.cert_file} has no URI SAN {SERVICE_SAN_PREFIX}<name>: the hub finds no service in it"
        )
    if len(named) > 1:
        raise CertificateProblem(f"{_CERT} {files.cert_file} names more than one service: the hub refuses it")
    if not _SERVICE_NAME.fullmatch(named[0]):
        raise CertificateProblem(
            f"{_CERT} {files.cert_file} names a service the hub refuses (1 to 128 characters of A-Z a-z 0-9 . _ -)"
        )
    return named[0]


def check_ca(tls: TLSConfig) -> None:
    """:class:`ConfigurationError` unless the CA file of tls (if any) holds a PEM certificate."""
    if tls.ca_file is None:
        return
    try:
        if not x509.load_pem_x509_certificates(Path(tls.ca_file).read_bytes()):
            raise ValueError("no certificate")
    except (OSError, ValueError) as e:
        raise ConfigurationError(
            f"the hubs' CA file (TLSConfig.ca_file, SNODRA_SERVER_TLS_CA) {tls.ca_file} cannot be used: it must hold "
            f"PEM certificates ({type(e).__name__})"
        ) from None


async def channel(
    address: str, tls: TLSConfig, pair: Pair | None, options: Sequence[tuple[str, int | str]]
) -> grpc.aio.Channel:
    """A TLS channel to address: the CA file read now (off the event loop), the server name to verify, and the client
    pair when there is one."""
    roots = None if tls.ca_file is None else await asyncio.to_thread(Path(tls.ca_file).read_bytes)
    credentials = grpc.ssl_channel_credentials(
        root_certificates=roots,
        private_key=None if pair is None else pair.key_pem,
        certificate_chain=None if pair is None else pair.chain_pem,
    )
    opts = list(options)
    if tls.server_name is not None:
        opts.append(("grpc.ssl_target_name_override", tls.server_name))
    return grpc.aio.secure_channel(address, credentials, options=opts)


class Certificates:
    """The client certificate files of an app: checked when built (:class:`ConfigurationError` for a pair that cannot
    be used or has ended), shared by every hub connection and read again at every connect and every
    ``cert_check_interval`` (:meth:`watch`).

    :meth:`refresh` adopts a different pair that is valid (the same checks, already valid, not ended) as
    :attr:`current` and bumps :attr:`generation`; one that is not is kept out, logged at debug when first seen and at
    error (once) when seen again, so a pair replaced in two writes is adopted once complete. It warns once per
    certificate when less than a tenth of its lifetime (a day at most) is left and no renewed file has appeared."""

    def __init__(self, files: ClientCertificate, service: str) -> None:
        self._files = files
        self._service = service
        try:
            pair = parse_pair(files, read_files(files), service)
        except CertificateProblem as e:
            raise ConfigurationError(str(e)) from None
        if pair.not_after <= time.time():
            raise ConfigurationError(
                f"{_CERT} {files.cert_file}: the certificate {pair.fingerprint} expired at {_when(pair.not_after)}"
            )
        self.current = pair
        self.generation = 0
        self._lock = threading.Lock()
        self._seen: str | None = None
        self._error_logged: str | None = None
        self._connect_warned: str | None = None
        self._warned_for: str | None = None
        log.info(
            "Hub client certificate %s loaded for service %s, valid until %s",
            pair.fingerprint,
            pair.service,
            _when(pair.not_after),
        )

    async def at_connect(self) -> tuple[int, Pair]:
        """The files read again (off the event loop): the generation and the pair a new stream presents."""
        return await asyncio.to_thread(self.refresh, connecting=True)

    def refresh(self, *, connecting: bool = False) -> tuple[int, Pair]:
        """Reads the files again; the generation and the pair to present now. Never raises."""
        with self._lock:
            try:
                raw = read_files(self._files)
                if raw != self.current.raw:
                    candidate = parse_pair(self._files, raw, self._service)
                    now = time.time()
                    if candidate.not_after <= now:
                        raise CertificateProblem(
                            f"{_CERT} {self._files.cert_file}: the new certificate {candidate.fingerprint} expired at "
                            f"{_when(candidate.not_after)}"
                        )
                    if candidate.not_before > now:
                        raise CertificateProblem(
                            f"{_CERT} {self._files.cert_file}: the new certificate {candidate.fingerprint} is not "
                            f"valid before {_when(candidate.not_before)}"
                        )
                    old, self.current = self.current, candidate
                    self.generation += 1
                    log.info(
                        "Hub client certificate of %s changed from %s to %s, valid until %s",
                        candidate.service,
                        old.fingerprint,
                        candidate.fingerprint,
                        _when(candidate.not_after),
                    )
                self._seen = self._error_logged = self._connect_warned = None
            except CertificateProblem as e:
                self._problem(str(e), connecting)
            self._near_end(self.current)
            return self.generation, self.current

    def _problem(self, problem: str, connecting: bool) -> None:
        fingerprint = self.current.fingerprint
        if connecting and problem != self._connect_warned:
            self._connect_warned = problem
            log.warning(
                "The hub client certificate files cannot be used, presenting the last good certificate %s: %s",
                fingerprint,
                problem,
            )
        if problem != self._seen:
            self._seen = problem
            log.debug("The hub client certificate files cannot be used yet, keeping %s: %s", fingerprint, problem)
        elif problem != self._error_logged:
            self._error_logged = problem
            log.error("The hub client certificate files cannot be used, keeping %s: %s", fingerprint, problem)

    def _near_end(self, pair: Pair) -> None:
        now = time.time()
        if pair.not_after <= now or self._warned_for == pair.fingerprint:
            return
        if pair.not_after - now < min((pair.not_after - pair.not_before) / 10, 24 * 3600.0):
            self._warned_for = pair.fingerprint
            log.warning(
                "Hub client certificate %s of %s ends at %s and no renewed file has appeared at %s: the hub closes the "
                "session at that moment",
                pair.fingerprint,
                pair.service,
                _when(pair.not_after),
                self._files.cert_file,
            )

    async def watch(
        self,
        connections: Callable[[], Sequence[HubConnection]],
        roll: Callable[[HubConnection], Awaitable[bool]],
        timings: Timings,
    ) -> None:
        """Every ``cert_check_interval``: reads the files, then rolls over to the current pair the connections whose
        registered stream presents an older one, one at a time in order. Before the first roll to a pair it waits, once
        per pair, a delay uniform in ``[0, roll_delay_bound(roll_delay_max, what the presented pair has left)]`` (so the
        replicas that see the same renewed file do not all leave at once), then reads the files again. A roll that does
        not complete ends the rollover to that pair (one error): the other hubs keep the old pair until the files
        change again."""
        # TODO(spec-gap): SG-4
        delayed = abandoned = -1
        reported: str | None = None
        while True:
            await asyncio.sleep(timings.cert_check_interval)
            try:
                generation, pair = await asyncio.to_thread(self.refresh)
                if generation == abandoned:
                    continue
                stale = [p for c in connections() if (p := c.presented) is not None and p[0] < generation]
                if not stale:
                    continue
                if generation != delayed:
                    left = min(presented.not_after for _, presented in stale) - time.time()
                    wait = roll_delay_bound(timings.roll_delay_max, left) * random.random()
                    if wait > 0:
                        log.info(
                            "Renewed client certificate %s seen: rolling %d hub(s) over to it in %.1f s",
                            pair.fingerprint,
                            len(stale),
                            wait,
                        )
                        await asyncio.sleep(wait)
                        generation, pair = await asyncio.to_thread(self.refresh)  # the files may have changed again
                        if generation == abandoned:
                            continue
                    delayed = generation
                for c in connections():
                    presented = c.presented
                    if presented is None or presented[0] >= generation:
                        continue
                    if not await roll(c):
                        abandoned = generation
                        log.error(
                            "The roll of hub %s over to the renewed client certificate %s did not complete within "
                            "%.0f s; the other hubs keep the old pair until the files change again",
                            c.address,
                            pair.fingerprint,
                            timings.roll_reconnect_limit,
                        )
                        break
                reported = None
            except Exception as e:  # never the end of the rotation: the next check runs
                problem = f"{type(e).__name__}: {e}"
                if problem != reported:
                    reported = problem
                    log.error("The client certificate check failed; it goes on at the next check", exc_info=True)
