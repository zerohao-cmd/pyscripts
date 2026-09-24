from __future__ import annotations

import subprocess
import zipfile
from pathlib import Path

from pyscripts.git_source import GitArtifactBuilder


def _git(repository: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _commit(repository: Path, message: str) -> str:
    _git(repository, "add", ".")
    _git(
        repository,
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.com",
        "commit",
        "-qm",
        message,
    )
    return _git(repository, "rev-parse", "HEAD")


def test_builder_archives_selected_branch_head(tmp_path: Path) -> None:
    repository = tmp_path / "repository"
    repository.mkdir()
    _git(repository, "init", "-q", "-b", "main")
    (repository / "version.txt").write_text("main", encoding="utf-8")
    main_revision = _commit(repository, "main")

    _git(repository, "switch", "-qc", "release/v1")
    (repository / "version.txt").write_text("release", encoding="utf-8")
    release_revision = _commit(repository, "release")

    builder = GitArtifactBuilder()
    with builder.build(str(repository), "main") as main_artifact:
        assert main_artifact.revision == main_revision
        with zipfile.ZipFile(main_artifact.path) as archive:
            assert archive.read("version.txt") == b"main"

    with builder.build(str(repository), "release/v1") as release_artifact:
        assert release_artifact.revision == release_revision
        with zipfile.ZipFile(release_artifact.path) as archive:
            assert archive.read("version.txt") == b"release"
