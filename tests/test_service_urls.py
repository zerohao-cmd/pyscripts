from __future__ import annotations

import pytest
from pydantic import ValidationError

from pyscripts.schemas import CreateServiceRequest, UpdateServiceRequest


@pytest.mark.parametrize(
    "url",
    [
        "https://git.example.com/team/project.git",
        "http://192.168.0.11:3000/team/project.git",
        "https://git.example.com/team/project",
        "http://[::1]:3000/team/project.git",
        " https://git.example.com/team/project.git ",
    ],
)
def test_service_repository_accepts_http_urls(url: str) -> None:
    created = CreateServiceRequest(name="test-service", git_url=url)
    updated = UpdateServiceRequest(git_url=url)
    assert created.git_url == updated.git_url == url.strip()


@pytest.mark.parametrize(
    "url",
    [
        "ssh://git@git.example.com/team/project.git",
        "git@git.example.com:team/project.git",
        "git://git.example.com/team/project.git",
        "file:///tmp/project",
        "/tmp/project",
        "../project",
        "ftp://git.example.com/project.git",
        "https:///project.git",
        "https://git.example.com:bad/project.git",
        "http://git.example.com:70000/project.git",
        "http://git.example.com:0/project.git",
        "https://git.example.com/team/project.git#main",
        "https://git.example.com/team/my project.git",
        "https://git.example.com\n/team/project.git",
        "",
    ],
)
def test_service_repository_rejects_non_http_or_invalid_urls(url: str) -> None:
    with pytest.raises(ValidationError, match="HTTP or HTTPS"):
        CreateServiceRequest(name="test-service", git_url=url)
    with pytest.raises(ValidationError):
        UpdateServiceRequest(git_url=url)


def test_service_update_requires_non_null_repository_if_provided() -> None:
    with pytest.raises(ValidationError, match="cannot be null"):
        UpdateServiceRequest(git_url=None)
    assert UpdateServiceRequest(tracking_mode="manual").git_url is None
