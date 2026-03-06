"""Tests for git panel API endpoints (routes/git.py).

Uses real git repos in tmp_path for integration testing.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _git(cwd: Path, *args: str) -> str:
    result = subprocess.run(
        ["git"] + list(args),
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _init_repo(path: Path, *, user: str = "Test User") -> Path:
    _git(path, "init", "-b", "main")
    _git(path, "config", "user.name", user)
    _git(path, "config", "user.email", "test@example.com")
    (path / "README.md").write_text("# Test\n")
    _git(path, "add", ".")
    _git(path, "commit", "-m", "Initial commit")
    return path


@pytest.fixture(autouse=True)
def _isolated_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Every test runs in an isolated git repo."""
    repo = _init_repo(tmp_path)
    monkeypatch.chdir(tmp_path)
    return repo


@pytest.fixture()
def client() -> TestClient:
    from haute.server import app

    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# GET /api/git/status
# ---------------------------------------------------------------------------


class TestGitStatus:
    def test_returns_main(self, client: TestClient) -> None:
        res = client.get("/api/git/status")
        assert res.status_code == 200
        body = res.json()
        assert body["branch"] == "main"
        assert body["is_main"] is True
        assert body["is_read_only"] is True

    def test_returns_changed_files(self, client: TestClient, tmp_path: Path) -> None:
        (tmp_path / "new.py").write_text("x = 1\n")
        res = client.get("/api/git/status")
        assert "new.py" in res.json()["changed_files"]

    def test_on_own_branch(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/feat")
        res = client.get("/api/git/status")
        body = res.json()
        assert body["branch"] == "pricing/test-user/feat"
        assert body["is_main"] is False
        assert body["is_read_only"] is False


# ---------------------------------------------------------------------------
# POST /api/git/branches — create
# ---------------------------------------------------------------------------


class TestGitCreateBranch:
    def test_creates_branch(self, client: TestClient, tmp_path: Path) -> None:
        res = client.post("/api/git/branches", json={"description": "Update factors"})
        assert res.status_code == 200
        body = res.json()
        assert body["branch"] == "pricing/test-user/update-factors"
        # Verify we're on the new branch
        assert _git(tmp_path, "symbolic-ref", "--short", "HEAD") == body["branch"]

    def test_empty_description(self, client: TestClient) -> None:
        res = client.post("/api/git/branches", json={"description": ""})
        assert res.status_code == 400

    def test_duplicate_name(self, client: TestClient, tmp_path: Path) -> None:
        client.post("/api/git/branches", json={"description": "my feature"})
        _git(tmp_path, "checkout", "main")
        res = client.post("/api/git/branches", json={"description": "my feature"})
        assert res.status_code == 400
        assert "already exists" in res.json()["detail"]


# ---------------------------------------------------------------------------
# GET /api/git/branches — list
# ---------------------------------------------------------------------------


class TestGitListBranches:
    def test_lists_branches(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/feat")
        _git(tmp_path, "checkout", "main")

        res = client.get("/api/git/branches")
        assert res.status_code == 200
        body = res.json()
        assert body["current"] == "main"
        names = [b["name"] for b in body["branches"]]
        assert "main" in names
        assert "pricing/test-user/feat" in names

    def test_own_branches_flagged(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/mine")
        _git(tmp_path, "checkout", "main")
        _git(tmp_path, "checkout", "-b", "pricing/other-user/theirs")
        _git(tmp_path, "checkout", "main")

        res = client.get("/api/git/branches")
        branches = {b["name"]: b for b in res.json()["branches"]}
        assert branches["pricing/test-user/mine"]["is_yours"] is True
        assert branches["pricing/other-user/theirs"]["is_yours"] is False


# ---------------------------------------------------------------------------
# POST /api/git/switch
# ---------------------------------------------------------------------------


class TestGitSwitch:
    def test_switches(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/feat")
        _git(tmp_path, "checkout", "main")

        res = client.post("/api/git/switch", json={"branch": "pricing/test-user/feat"})
        assert res.status_code == 200
        assert _git(tmp_path, "symbolic-ref", "--short", "HEAD") == "pricing/test-user/feat"

    def test_auto_commits_dirty(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/feat")
        (tmp_path / "dirty.py").write_text("x = 1\n")

        res = client.post("/api/git/switch", json={"branch": "main"})
        assert res.status_code == 200

        # Switch back and verify the file is there
        _git(tmp_path, "checkout", "pricing/test-user/feat")
        assert (tmp_path / "dirty.py").exists()


# ---------------------------------------------------------------------------
# POST /api/git/save
# ---------------------------------------------------------------------------


class TestGitSave:
    def test_saves(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/feat")
        (tmp_path / "main.py").write_text("x = 1\n")

        res = client.post("/api/git/save")
        assert res.status_code == 200
        body = res.json()
        assert body["commit_sha"]
        assert body["message"] == "Updated main"

    def test_blocked_on_main(self, client: TestClient, tmp_path: Path) -> None:
        (tmp_path / "new.py").write_text("x = 1\n")
        res = client.post("/api/git/save")
        assert res.status_code == 403

    def test_no_changes(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/feat")
        res = client.post("/api/git/save")
        assert res.status_code == 400
        assert "No changes" in res.json()["detail"]


# ---------------------------------------------------------------------------
# POST /api/git/submit
# ---------------------------------------------------------------------------


class TestGitSubmit:
    def test_submit_returns_branch(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/feat")
        res = client.post("/api/git/submit")
        assert res.status_code == 200
        assert res.json()["branch"] == "pricing/test-user/feat"

    def test_blocked_on_main(self, client: TestClient) -> None:
        res = client.post("/api/git/submit")
        assert res.status_code == 403


# ---------------------------------------------------------------------------
# GET /api/git/history
# ---------------------------------------------------------------------------


class TestGitHistory:
    def test_returns_entries(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/feat")
        (tmp_path / "a.py").write_text("a = 1\n")
        _git(tmp_path, "add", ".")
        _git(tmp_path, "commit", "-m", "Add a.py")

        res = client.get("/api/git/history")
        assert res.status_code == 200
        entries = res.json()["entries"]
        assert len(entries) == 1
        assert entries[0]["message"] == "Add a.py"

    def test_respects_limit(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/feat")
        for i in range(5):
            (tmp_path / f"f{i}.py").write_text(f"x = {i}\n")
            _git(tmp_path, "add", ".")
            _git(tmp_path, "commit", "-m", f"Commit {i}")

        res = client.get("/api/git/history?limit=3")
        assert len(res.json()["entries"]) == 3


# ---------------------------------------------------------------------------
# POST /api/git/revert
# ---------------------------------------------------------------------------


class TestGitRevert:
    def test_reverts(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/feat")
        (tmp_path / "a.py").write_text("v1\n")
        _git(tmp_path, "add", ".")
        _git(tmp_path, "commit", "-m", "v1")
        target = _git(tmp_path, "rev-parse", "HEAD")

        (tmp_path / "a.py").write_text("v2\n")
        _git(tmp_path, "add", ".")
        _git(tmp_path, "commit", "-m", "v2")

        res = client.post("/api/git/revert", json={"sha": target})
        assert res.status_code == 200
        assert res.json()["reverted_to"] == target[:7]
        assert (tmp_path / "a.py").read_text() == "v1\n"

    def test_blocked_on_main(self, client: TestClient, tmp_path: Path) -> None:
        sha = _git(tmp_path, "rev-parse", "HEAD")
        res = client.post("/api/git/revert", json={"sha": sha})
        assert res.status_code == 403

    def test_invalid_sha(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/feat")
        res = client.post("/api/git/revert", json={"sha": "deadbeef12345678"})
        assert res.status_code == 400


# ---------------------------------------------------------------------------
# POST /api/git/pull
# ---------------------------------------------------------------------------


class TestGitPull:
    def test_blocked_on_main(self, client: TestClient) -> None:
        res = client.post("/api/git/pull")
        # main is protected → 403
        assert res.status_code in (400, 403)

    def test_no_remote(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/feat")
        res = client.post("/api/git/pull")
        assert res.status_code == 400
        assert "No remote" in res.json()["detail"]


# ---------------------------------------------------------------------------
# POST /api/git/archive
# ---------------------------------------------------------------------------


class TestGitArchive:
    def test_archives(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/old")
        _git(tmp_path, "checkout", "main")

        res = client.post("/api/git/archive", json={"branch": "pricing/test-user/old"})
        assert res.status_code == 200
        assert res.json()["archived_as"].startswith("archive/")

    def test_blocked_on_protected(self, client: TestClient) -> None:
        res = client.post("/api/git/archive", json={"branch": "main"})
        assert res.status_code == 403


# ---------------------------------------------------------------------------
# DELETE /api/git/branches
# ---------------------------------------------------------------------------


class TestGitDeleteBranch:
    def test_deletes(self, client: TestClient, tmp_path: Path) -> None:
        _git(tmp_path, "checkout", "-b", "pricing/test-user/old")
        _git(tmp_path, "checkout", "main")

        res = client.request("DELETE", "/api/git/branches", json={"branch": "pricing/test-user/old"})
        assert res.status_code == 200
        branches = _git(tmp_path, "branch")
        assert "old" not in branches

    def test_blocked_on_protected(self, client: TestClient) -> None:
        res = client.request("DELETE", "/api/git/branches", json={"branch": "main"})
        assert res.status_code == 403
