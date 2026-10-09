from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass

from pyscripts.config import Settings


def _urlsafe_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _urlsafe_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


@dataclass(frozen=True, slots=True)
class AdminSession:
    username: str
    expires_at: int
    credential_version: int


def hash_admin_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=2**14,
        r=8,
        p=1,
        dklen=32,
    )
    return f"scrypt$16384$8$1${_urlsafe_encode(salt)}${_urlsafe_encode(digest)}"


def verify_admin_password(password: str, encoded: str) -> bool:
    try:
        algorithm, n, r, p, salt, expected = encoded.split("$", 5)
        if algorithm != "scrypt":
            return False
        digest = hashlib.scrypt(
            password.encode("utf-8"),
            salt=_urlsafe_decode(salt),
            n=int(n),
            r=int(r),
            p=int(p),
            dklen=32,
        )
        expected_digest = _urlsafe_decode(expected)
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(digest, expected_digest)


class AdminAuth:
    """Single-administrator session signing with a database-backed password."""

    def __init__(self, settings: Settings):
        self.username = settings.admin_username
        self.cookie_name = settings.admin_cookie_name
        self.ttl_seconds = settings.admin_session_ttl_seconds
        self.cookie_secure = settings.admin_cookie_secure
        self.bootstrap_password = (
            settings.admin_password.get_secret_value()
            if settings.admin_password is not None
            else None
        )
        self._secret = (
            settings.admin_session_secret.get_secret_value().encode("utf-8")
            if settings.admin_session_secret is not None
            else None
        )

    @property
    def configured(self) -> bool:
        return bool(self.username == "admin" and self._secret and len(self._secret) >= 32)

    def issue_session(
        self,
        credential_version: int,
        *,
        now: int | None = None,
    ) -> str:
        if not self.configured or self._secret is None:
            raise RuntimeError("administrator authentication is not configured")
        issued_at = int(time.time()) if now is None else now
        payload = _urlsafe_encode(
            json.dumps(
                {
                    "sub": self.username,
                    "iat": issued_at,
                    "exp": issued_at + self.ttl_seconds,
                    "ver": credential_version,
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
        signature = _urlsafe_encode(
            hmac.new(self._secret, payload.encode("ascii"), hashlib.sha256).digest()
        )
        return f"{payload}.{signature}"

    def verify_session(
        self, token: str | None, *, now: int | None = None
    ) -> AdminSession | None:
        if not self.configured or self._secret is None or not token:
            return None
        try:
            payload, signature = token.split(".", 1)
            expected = _urlsafe_encode(
                hmac.new(self._secret, payload.encode("ascii"), hashlib.sha256).digest()
            )
            if not hmac.compare_digest(expected, signature):
                return None
            value = json.loads(_urlsafe_decode(payload))
            username = value["sub"]
            expires_at = int(value["exp"])
            credential_version = int(value["ver"])
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            return None
        current_time = int(time.time()) if now is None else now
        if username != self.username or expires_at <= current_time:
            return None
        return AdminSession(
            username=username,
            expires_at=expires_at,
            credential_version=credential_version,
        )
