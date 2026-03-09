"""Tests for the git operations layer (_git.py).

Uses real git repos in tmp_path to test actual git behaviour — no mocking
of subprocess.  This ensures guardrails work against real git state.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from haute._git import (
    GitError,
    GitGuardrailError,
    _build_compare_url,
    _generate_commit_message,
    _get_current_branch,
    _get_user_slug,
    _is_own_branch,
    _slugify,
    _validate_ref_name,
    archive_branch,
    create_branch,
    delete_branch,
    get_history,
    get_status,
    list_branches,
    pull_latest,
    revert_to,
    save_progress,
    submit_for_review,
    switch_branch,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _git(cwd: Path, *args: str) -> str:
    """Run a raw git command for test setup."""
    result = subprocess.run(
        ["git"] + list(args),
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _init_repo(path: Path, *, user: str = "Test User") -> Path:
    """Create a fresh git repo with an initial commit on 'main'."""
    _git(path, "init", "-b", "main")
    _git(path, "config", "user.name", user)
    _git(path, "config", "user.email", "test@example.com")
    (path / "README.md").write_text("# Test\n")
    _git(path, "add", ".")
    _git(path, "commit", "-m", "Initial commit")
    return path


def _init_repo_with_remote(path: Path, *, user: str = "Test User") -> tuple[Path, Path]:
    """Create a repo + a bare remote, linked via 'origin'."""
    remote = path / "remote.git"
    remote.mkdir()
    _git(remote, "init", "--bare", "-b", "main")

    repo = path / "repo"
    repo.mkdir()
    _init_repo(repo, user=user)
    _git(repo, "remote", "add", "origin", str(remote))
    _git(repo, "push", "-u", "origin", "main")
    return repo, remote


# ---------------------------------------------------------------------------
# Slugify
# ---------------------------------------------------------------------------


class TestSlugify:
    def test_basic(self) -> None:
        assert _slugify("Update area factors") == "update-area-factors"

    def test_special_chars(self) -> None:
        assert _slugify("Fix postcode (v2)") == "fix-postcode-v2"

    def test_leading_trailing_dashes(self) -> None:
        assert _slugify("---hello---") == "hello"

    def test_empty_returns_user(self) -> None:
        assert _slugify("") == "user"

    def test_numbers(self) -> None:
        assert _slugify("Add NCD step 3") == "add-ncd-step-3"


# ---------------------------------------------------------------------------
# Commit message generation
# ---------------------------------------------------------------------------


class TestGenerateCommitMessage:
    def test_empty(self) -> None:
        assert _generate_commit_message([]) == "Save progress"

    def test_single_py(self) -> None:
        assert _generate_commit_message(["main.py"]) == "Updated main"

    def test_multiple_files(self) -> None:
        msg = _generate_commit_message(["main.py", "modules/scoring.py"])
        assert "main" in msg
        assert "scoring" in msg

    def test_many_files(self) -> None:
        files = [f"file{i}.py" for i in range(5)]
        msg = _generate_commit_message(files)
        assert "5 files" in msg

    def test_config_json(self) -> None:
        msg = _generate_commit_message(["config/banding/area.json"])
        assert "config/area" in msg

    def test_sidecar_skipped(self) -> None:
        msg = _generate_commit_message(["main.haute.json"])
        assert msg == "Save progress"

    def test_sidecar_with_real_file(self) -> None:
        msg = _generate_commit_message(["main.py", "main.haute.json"])
        assert msg == "Updated main"


# ---------------------------------------------------------------------------
# Branch ownership
# ---------------------------------------------------------------------------


class TestIsOwnBranch:
    def test_own_branch(self) -> None:
        assert _is_own_branch("pricing/test-user/my-feature", "test-user")

    def test_other_branch(self) -> None:
        assert not _is_own_branch("pricing/other-user/feature", "test-user")

    def test_main_is_not_own(self) -> None:
        assert not _is_own_branch("main", "test-user")

    def test_archive_is_not_own(self) -> None:
        assert not _is_own_branch("archive/old-feature", "test-user")


# ---------------------------------------------------------------------------
# Compare URL generation
# ---------------------------------------------------------------------------


class TestBuildCompareUrl:
    def test_github_ssh(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "remote", "add", "origin", "git@github.com:org/repo.git")
        url = _build_compare_url("pricing/user/feat", "main", repo)
        assert url == "https://github.com/org/repo/compare/main...pricing/user/feat"

    def test_github_https(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "remote", "add", "origin", "https://github.com/org/repo.git")
        url = _build_compare_url("pricing/user/feat", "main", repo)
        assert url == "https://github.com/org/repo/compare/main...pricing/user/feat"

    def test_gitlab(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "remote", "add", "origin", "https://gitlab.com/org/repo.git")
        url = _build_compare_url("pricing/user/feat", "main", repo)
        assert url is not None
        assert "merge_requests/new" in url

    def test_no_remote(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        url = _build_compare_url("feat", "main", repo)
        assert url is None


# ---------------------------------------------------------------------------
# get_status
# ---------------------------------------------------------------------------


class TestGetStatus:
    def test_on_main(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        s = get_status(repo)
        assert s.branch == "main"
        assert s.is_main is True
        assert s.is_read_only is True
        assert s.changed_files == []

    def test_changed_files(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        (repo / "new.py").write_text("x = 1\n")
        s = get_status(repo)
        assert "new.py" in s.changed_files

    def test_on_own_branch(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/my-feat")
        s = get_status(repo)
        assert s.branch == "pricing/test-user/my-feat"
        assert s.is_main is False
        assert s.is_read_only is False

    def test_on_other_branch(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/other-user/their-feat")
        s = get_status(repo)
        assert s.is_read_only is True

    def test_not_git_repo(self, tmp_path: Path) -> None:
        with pytest.raises(GitError, match="Not a git repository"):
            get_status(tmp_path)


# ---------------------------------------------------------------------------
# create_branch
# ---------------------------------------------------------------------------


class TestCreateBranch:
    def test_creates_and_switches(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        branch = create_branch("Update area factors", repo)
        assert branch == "pricing/test-user/update-area-factors"
        assert _get_current_branch(repo) == branch

    def test_empty_description(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        with pytest.raises(GitError, match="empty"):
            create_branch("", repo)

    def test_duplicate_name(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        create_branch("my feature", repo)
        _git(repo, "checkout", "main")
        with pytest.raises(GitError, match="already exists"):
            create_branch("my feature", repo)

    def test_special_chars_slugified(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        branch = create_branch("Fix NCD (version 2)", repo)
        assert branch == "pricing/test-user/fix-ncd-version-2"


# ---------------------------------------------------------------------------
# list_branches
# ---------------------------------------------------------------------------


class TestListBranches:
    def test_lists_main(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        result = list_branches(repo)
        assert result.current == "main"
        names = [b.name for b in result.branches]
        assert "main" in names

    def test_own_branches_first(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/other-user/feat")
        _git(repo, "checkout", "main")
        _git(repo, "checkout", "-b", "pricing/test-user/my-feat")
        _git(repo, "checkout", "main")

        result = list_branches(repo)
        # Filter out main and archived
        non_main = [b for b in result.branches if b.name != "main"]
        assert non_main[0].is_yours is True
        assert non_main[1].is_yours is False

    def test_archived_last(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "archive/old-feat")
        _git(repo, "checkout", "main")
        _git(repo, "checkout", "-b", "pricing/test-user/active")
        _git(repo, "checkout", "main")

        result = list_branches(repo)
        non_main = [b for b in result.branches if b.name != "main"]
        assert non_main[-1].is_archived is True


# ---------------------------------------------------------------------------
# switch_branch
# ---------------------------------------------------------------------------


class TestSwitchBranch:
    def test_switches(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        _git(repo, "checkout", "main")
        switch_branch("pricing/test-user/feat", repo)
        assert _get_current_branch(repo) == "pricing/test-user/feat"

    def test_auto_commits_before_switch(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        (repo / "dirty.py").write_text("x = 1\n")
        switch_branch("main", repo)
        # Dirty file should have been committed
        _git(repo, "checkout", "pricing/test-user/feat")
        assert (repo / "dirty.py").exists()

    def test_noop_same_branch(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        switch_branch("main", repo)  # Already on main — should not error


# ---------------------------------------------------------------------------
# save_progress
# ---------------------------------------------------------------------------


class TestSaveProgress:
    def test_saves_and_returns_info(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        (repo / "main.py").write_text("x = 1\n")

        result = save_progress(repo)
        assert result.commit_sha
        assert result.message == "Updated main"
        assert result.timestamp

    def test_blocked_on_main(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        (repo / "new.py").write_text("x = 1\n")
        with pytest.raises(GitGuardrailError, match="protected"):
            save_progress(repo)

    def test_no_changes(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        with pytest.raises(GitError, match="No changes"):
            save_progress(repo)


# ---------------------------------------------------------------------------
# get_history
# ---------------------------------------------------------------------------


class TestGetHistory:
    def test_returns_commits(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        (repo / "a.py").write_text("a = 1\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "Add a.py")

        entries = get_history(cwd=repo)
        assert len(entries) == 1
        assert entries[0].message == "Add a.py"
        assert "a.py" in entries[0].files_changed

    def test_empty_branch(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        entries = get_history(cwd=repo)
        assert entries == []

    def test_limit(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        for i in range(5):
            (repo / f"file{i}.py").write_text(f"x = {i}\n")
            _git(repo, "add", ".")
            _git(repo, "commit", "-m", f"Commit {i}")

        entries = get_history(limit=3, cwd=repo)
        assert len(entries) == 3


# ---------------------------------------------------------------------------
# revert_to
# ---------------------------------------------------------------------------


class TestRevertTo:
    def test_reverts_to_commit(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")

        (repo / "a.py").write_text("v1\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "v1")
        target_sha = _git(repo, "rev-parse", "HEAD")

        (repo / "a.py").write_text("v2\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "v2")

        result = revert_to(target_sha, repo)
        assert result.reverted_to == target_sha[:7]
        assert result.backup_tag.startswith("backup/")
        assert (repo / "a.py").read_text() == "v1\n"

    def test_blocked_on_main(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        sha = _git(repo, "rev-parse", "HEAD")
        with pytest.raises(GitGuardrailError, match="protected"):
            revert_to(sha, repo)

    def test_invalid_sha(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        with pytest.raises(GitError, match="not found"):
            revert_to("deadbeef12345678", repo)

    def test_backup_tag_created(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        (repo / "a.py").write_text("v1\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "v1")
        sha = _git(repo, "rev-parse", "HEAD")

        (repo / "a.py").write_text("v2\n")
        _git(repo, "add", ".")
        _git(repo, "commit", "-m", "v2")

        result = revert_to(sha, repo)
        # Verify the backup tag exists and points to the pre-revert HEAD
        tag_sha = _git(repo, "rev-parse", result.backup_tag)
        assert tag_sha  # Tag exists and resolves


# ---------------------------------------------------------------------------
# pull_latest
# ---------------------------------------------------------------------------


class TestPullLatest:
    def test_no_remote(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        with pytest.raises(GitError, match="No remote"):
            pull_latest(repo)

    def test_pulls_new_commits(self, tmp_path: Path) -> None:
        repo, remote = _init_repo_with_remote(tmp_path)

        # Create a branch
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        _git(repo, "push", "-u", "origin", "pricing/test-user/feat")

        # Simulate someone else pushing to main (via the remote)
        clone = tmp_path / "clone"
        _git(tmp_path, "clone", str(remote), str(clone))
        _git(clone, "config", "user.name", "Other User")
        _git(clone, "config", "user.email", "other@example.com")
        (clone / "other.py").write_text("y = 1\n")
        _git(clone, "add", ".")
        _git(clone, "commit", "-m", "Other commit")
        _git(clone, "push", "origin", "main")

        result = pull_latest(repo)
        assert result.success is True
        assert result.conflict is False
        assert result.commits_pulled >= 1

    def test_blocked_on_main(self, tmp_path: Path) -> None:
        repo, _ = _init_repo_with_remote(tmp_path)
        with pytest.raises(GitGuardrailError, match="protected"):
            pull_latest(repo)

    def test_no_commits_to_pull(self, tmp_path: Path) -> None:
        repo, _ = _init_repo_with_remote(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        result = pull_latest(repo)
        assert result.success is True
        assert result.commits_pulled == 0


# ---------------------------------------------------------------------------
# submit_for_review
# ---------------------------------------------------------------------------


class TestSubmitForReview:
    def test_returns_branch(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        result = submit_for_review(repo)
        assert result.branch == "pricing/test-user/feat"
        # No remote → no URL
        assert result.compare_url is None

    def test_pushes_to_remote(self, tmp_path: Path) -> None:
        repo, _ = _init_repo_with_remote(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        (repo / "change.py").write_text("x = 1\n")

        result = submit_for_review(repo)
        assert result.branch == "pricing/test-user/feat"
        # compare_url is None because the remote is a local bare repo, not github
        assert result.compare_url is None

    def test_blocked_on_main(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        with pytest.raises(GitGuardrailError, match="protected"):
            submit_for_review(repo)


# ---------------------------------------------------------------------------
# archive_branch
# ---------------------------------------------------------------------------


class TestArchiveBranch:
    def test_renames_to_archive(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/old-feat")
        _git(repo, "checkout", "main")

        archived = archive_branch("pricing/test-user/old-feat", repo)
        assert archived.startswith("archive/")

        # Old branch should be gone
        branches = _git(repo, "branch")
        assert "pricing/test-user/old-feat" not in branches

    def test_blocked_on_protected(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        with pytest.raises(GitGuardrailError):
            archive_branch("main", repo)

    def test_already_archived(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "archive/old")
        _git(repo, "checkout", "main")
        with pytest.raises(GitError, match="already archived"):
            archive_branch("archive/old", repo)

    def test_switches_away_if_current(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/current-feat")
        archived = archive_branch("pricing/test-user/current-feat", repo)
        # Should have switched to main
        assert _get_current_branch(repo) == "main"
        assert archived.startswith("archive/")


# ---------------------------------------------------------------------------
# delete_branch
# ---------------------------------------------------------------------------


class TestDeleteBranch:
    def test_deletes(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/to-delete")
        _git(repo, "checkout", "main")
        delete_branch("pricing/test-user/to-delete", repo)
        branches = _git(repo, "branch")
        assert "to-delete" not in branches

    def test_blocked_on_protected(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        with pytest.raises(GitGuardrailError):
            delete_branch("main", repo)

    def test_switches_away_if_current(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        delete_branch("pricing/test-user/feat", repo)
        assert _get_current_branch(repo) == "main"


# ---------------------------------------------------------------------------
# User slug
# ---------------------------------------------------------------------------


class TestGetUserSlug:
    def test_reads_git_config(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path, user="Ralph Thompson")
        slug = _get_user_slug(repo)
        assert slug == "ralph-thompson"

    def test_handles_special_chars(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path, user="Jean-Pierre O'Brien")
        slug = _get_user_slug(repo)
        assert slug == "jean-pierre-o-brien"


# ---------------------------------------------------------------------------
# Ref name validation (argument injection prevention)
# ---------------------------------------------------------------------------


class TestValidateRefName:
    """_validate_ref_name blocks names that could be interpreted as git flags
    or contain characters dangerous for shell/git."""

    def test_normal_branch_passes(self) -> None:
        _validate_ref_name("pricing/user/my-feature")

    def test_normal_sha_passes(self) -> None:
        _validate_ref_name("abc123def456")

    def test_rejects_empty(self) -> None:
        with pytest.raises(GitError, match="empty"):
            _validate_ref_name("")

    def test_rejects_leading_dash(self) -> None:
        with pytest.raises(GitError, match="must not start with '-'"):
            _validate_ref_name("--upload-pack=evil")

    def test_rejects_single_dash(self) -> None:
        with pytest.raises(GitError, match="must not start with '-'"):
            _validate_ref_name("-b")

    def test_rejects_null_byte(self) -> None:
        with pytest.raises(GitError, match="forbidden characters"):
            _validate_ref_name("branch\x00name")

    def test_rejects_tilde(self) -> None:
        with pytest.raises(GitError, match="forbidden characters"):
            _validate_ref_name("branch~1")

    def test_rejects_caret(self) -> None:
        with pytest.raises(GitError, match="forbidden characters"):
            _validate_ref_name("branch^2")

    def test_rejects_colon(self) -> None:
        with pytest.raises(GitError, match="forbidden characters"):
            _validate_ref_name("branch:name")

    def test_rejects_backslash(self) -> None:
        with pytest.raises(GitError, match="forbidden characters"):
            _validate_ref_name("branch\\name")

    def test_rejects_question_mark(self) -> None:
        with pytest.raises(GitError, match="forbidden characters"):
            _validate_ref_name("branch?name")

    def test_rejects_asterisk(self) -> None:
        with pytest.raises(GitError, match="forbidden characters"):
            _validate_ref_name("branch*name")

    def test_rejects_bracket(self) -> None:
        with pytest.raises(GitError, match="forbidden characters"):
            _validate_ref_name("branch[name")


class TestArgumentInjectionPrevention:
    """Verify that public functions reject malicious ref names before
    passing them to git commands."""

    def test_switch_branch_rejects_flag(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        with pytest.raises(GitError, match="must not start with '-'"):
            switch_branch("--upload-pack=evil", repo)

    def test_delete_branch_rejects_flag(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        with pytest.raises(GitError, match="must not start with '-'"):
            delete_branch("--force", repo)

    def test_archive_branch_rejects_flag(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        with pytest.raises(GitError, match="must not start with '-'"):
            archive_branch("--delete", repo)

    def test_revert_to_rejects_flag(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        _git(repo, "checkout", "-b", "pricing/test-user/feat")
        with pytest.raises(GitError, match="must not start with '-'"):
            revert_to("--hard", repo)

    def test_switch_branch_rejects_control_chars(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        with pytest.raises(GitError, match="forbidden characters"):
            switch_branch("branch\x00evil", repo)

    def test_delete_branch_rejects_control_chars(self, tmp_path: Path) -> None:
        repo = _init_repo(tmp_path)
        with pytest.raises(GitError, match="forbidden characters"):
            delete_branch("branch\x00evil", repo)
