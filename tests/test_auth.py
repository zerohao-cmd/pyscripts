from __future__ import annotations

from pydantic import SecretStr

from pyscripts.auth import AdminAuth, hash_admin_password, verify_admin_password
from pyscripts.config import Settings


def test_admin_session_is_signed_and_expires() -> None:
    auth = AdminAuth(
        Settings(
            database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
            admin_password=SecretStr("correct horse battery staple"),
            admin_session_secret=SecretStr("a" * 32),
            admin_session_ttl_seconds=300,
        )
    )

    password_hash = hash_admin_password("correct horse battery staple")
    assert verify_admin_password("correct horse battery staple", password_hash)
    assert not verify_admin_password("wrong", password_hash)
    token = auth.issue_session(credential_version=3, now=1_000)
    session = auth.verify_session(token, now=1_299)
    assert session is not None
    assert session.username == "admin"
    assert session.credential_version == 3
    assert auth.verify_session(token, now=1_300) is None
    assert auth.verify_session(f"{token[:-1]}x", now=1_001) is None


def test_admin_auth_refuses_incomplete_configuration() -> None:
    auth = AdminAuth(
        Settings(
            database_url=SecretStr("sqlite+aiosqlite:///:memory:"),
            admin_password=None,
            admin_session_secret=None,
        )
    )

    assert not auth.configured
