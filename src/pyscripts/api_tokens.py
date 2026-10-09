from __future__ import annotations

import hashlib
import secrets

TOKEN_PREFIX = "pst_"


def issue_api_token() -> tuple[str, str, str]:
    token = f"{TOKEN_PREFIX}{secrets.token_urlsafe(32)}"
    return token, hash_api_token(token), token[:12]


def hash_api_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def bearer_token(authorization: str | None) -> str | None:
    if not authorization:
        return None
    scheme, separator, value = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer" or not value.strip():
        return None
    return value.strip()
