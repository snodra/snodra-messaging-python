"""Test certificates: a CA that issues hub (server) and service (client) certificates.

Keys are RSA 2048; a certificate is valid for one hour from a minute ago unless its end is given. Making a key
takes 10 to 60 ms, too long to do on a test's event loop: a background thread keeps a few fresh keys ready
(generating one releases the GIL), so the functions here stay synchronous and quick. No key is used twice.
A service certificate names its service the way the hub reads it: one URI SAN
``snodra://service/<name>`` (https://snodra.damoreira.com/docs/library-authors/protocol/).
"""

from __future__ import annotations

import queue
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

VALIDITY = timedelta(hours=1)
_BACKDATE = timedelta(minutes=1)


@dataclass(frozen=True)
class KeyPair:
    """A private key and its certificate."""

    key: rsa.RSAPrivateKey
    certificate: x509.Certificate

    @property
    def cert_pem(self) -> bytes:
        return self.certificate.public_bytes(serialization.Encoding.PEM)

    @property
    def key_pem(self) -> bytes:
        return self.key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        )


@dataclass(frozen=True)
class Ca:
    """A self-signed certificate authority."""

    name: str
    key: rsa.RSAPrivateKey
    certificate: x509.Certificate

    @property
    def cert_pem(self) -> bytes:
        return self.certificate.public_bytes(serialization.Encoding.PEM)

    def issue_server(self, dns_name: str) -> KeyPair:
        """A hub certificate for dns_name (its only SAN)."""
        return self._issue(
            dns_name,
            x509.SubjectAlternativeName([x509.DNSName(dns_name)]),
            ExtendedKeyUsageOID.SERVER_AUTH,
            not_after=None,
        )

    def issue_client(self, service: str, *, not_after: datetime | None = None) -> KeyPair:
        """A certificate of service; not_after may lie in the past (an expired certificate)."""
        return self._issue(
            service,
            x509.SubjectAlternativeName([x509.UniformResourceIdentifier(f"snodra://service/{service}")]),
            ExtendedKeyUsageOID.CLIENT_AUTH,
            not_after=not_after,
        )

    def _issue(
        self,
        common_name: str,
        sans: x509.SubjectAlternativeName,
        usage: x509.ObjectIdentifier,
        *,
        not_after: datetime | None,
    ) -> KeyPair:
        key = _new_key()
        now = datetime.now(UTC)
        not_before = now - _BACKDATE
        if not_after is None:
            not_after = now + VALIDITY
        else:
            not_before = min(not_before, not_after - VALIDITY)
        certificate = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
            .issuer_name(self.certificate.subject)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(not_before)
            .not_valid_after(not_after)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=True,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=False,
                    crl_sign=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
            .add_extension(sans, critical=False)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(self.key.public_key()), critical=False)
            .sign(self.key, hashes.SHA256())
        )
        return KeyPair(key, certificate)


@dataclass(frozen=True)
class ServerTls:
    """How a fake hub serves TLS: its key pair, and the CA client certificates must chain to
    (None: no client certificate is asked for)."""

    key_pair: KeyPair
    client_ca: Ca | None


def make_ca(name: str) -> Ca:
    """A CA named name (its common name), valid for one hour."""
    key = _new_key()
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - _BACKDATE)
        .not_valid_after(now + VALIDITY)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )
    return Ca(name, key, certificate)


_KEYS: queue.Queue[rsa.RSAPrivateKey] = queue.Queue(maxsize=8)


def _make_keys() -> None:
    while True:
        _KEYS.put(rsa.generate_private_key(public_exponent=65537, key_size=2048))


threading.Thread(target=_make_keys, name="certs:keys", daemon=True).start()


def _new_key() -> rsa.RSAPrivateKey:
    """A fresh key from the pool; it waits only when more keys are asked for at once than the pool holds."""
    return _KEYS.get()
