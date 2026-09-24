from __future__ import annotations

import hashlib
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path


class GitSourceError(RuntimeError):
    pass


@dataclass(slots=True)
class GitArtifact:
    revision: str
    path: Path
    digest: str
    _temporary_root: Path

    @property
    def uri(self) -> str:
        return self.path.as_uri()

    def cleanup(self) -> None:
        shutil.rmtree(self._temporary_root, ignore_errors=True)

    def __enter__(self) -> GitArtifact:
        return self

    def __exit__(self, *_: object) -> None:
        self.cleanup()


class GitArtifactBuilder:
    """Build an immutable ZIP from the selected branch head."""

    def build(self, git_url: str, branch: str | None = None) -> GitArtifact:
        temporary_root = Path(tempfile.mkdtemp(prefix="pyscripts-git-"))
        checkout = temporary_root / "checkout"
        artifact = temporary_root / "artifact.zip"
        try:
            clone_command = [
                "git",
                "clone",
                "--depth",
                "1",
                "--no-tags",
            ]
            if branch is not None:
                clone_command.extend(("--branch", branch, "--single-branch"))
            clone_command.extend(("--", git_url, str(checkout)))
            self._run(*clone_command, cwd=temporary_root)
            revision = self._run(
                "git",
                "rev-parse",
                "HEAD",
                cwd=checkout,
            ).strip()
            if len(revision) != 40 or any(
                character not in "0123456789abcdef" for character in revision
            ):
                raise GitSourceError("Git HEAD did not resolve to a full commit SHA")
            self._run(
                "git",
                "archive",
                "--format=zip",
                f"--output={artifact}",
                revision,
                cwd=checkout,
            )
            digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
            return GitArtifact(revision, artifact, digest, temporary_root)
        except BaseException:
            shutil.rmtree(temporary_root, ignore_errors=True)
            raise

    @staticmethod
    def _run(*command: str, cwd: Path) -> str:
        try:
            completed = subprocess.run(
                command,
                cwd=cwd,
                check=False,
                capture_output=True,
                text=True,
                timeout=120,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise GitSourceError(f"Git operation failed: {error}") from error
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout).strip()
            raise GitSourceError(
                f"Git operation failed: {detail[:2000] or 'unknown error'}"
            )
        return completed.stdout
