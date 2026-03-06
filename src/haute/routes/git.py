"""Git panel endpoints — branch management, save, revert, and submit.

Provides a simplified git workflow for pricing analysts who don't use
git directly.  All operations go through ``haute._git`` which enforces
guardrails (no writes to protected branches, backup tags before revert).
"""

from __future__ import annotations

from typing import NoReturn

from fastapi import APIRouter, HTTPException

from haute._git import (
    GitError,
    GitGuardrailError,
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
from haute._logging import get_logger
from haute.schemas import (
    GitArchiveRequest,
    GitArchiveResponse,
    GitBranchItem,
    GitBranchListResponse,
    GitCreateBranchRequest,
    GitCreateBranchResponse,
    GitDeleteBranchRequest,
    GitHistoryEntry,
    GitHistoryResponse,
    GitPullResponse,
    GitRevertRequest,
    GitRevertResponse,
    GitSaveResponse,
    GitStatusResponse,
    GitSubmitResponse,
    GitSwitchBranchRequest,
)

logger = get_logger(component="server.git")

router = APIRouter(prefix="/api/git", tags=["git"])


def _handle_git_error(e: GitError) -> NoReturn:
    """Convert git errors to appropriate HTTP responses."""
    if isinstance(e, GitGuardrailError):
        raise HTTPException(status_code=403, detail=str(e))
    raise HTTPException(status_code=400, detail=str(e))


# ---------------------------------------------------------------------------
# GET /api/git/status
# ---------------------------------------------------------------------------


@router.get("/status", response_model=GitStatusResponse)
async def git_status() -> GitStatusResponse:
    """Current branch, changed files, and main-ahead status."""
    try:
        s = get_status()
    except GitError as e:
        _handle_git_error(e)
    return GitStatusResponse(
        branch=s.branch,
        is_main=s.is_main,
        is_read_only=s.is_read_only,
        changed_files=s.changed_files,
        main_ahead=s.main_ahead,
        main_ahead_by=s.main_ahead_by,
        main_last_updated=s.main_last_updated,
    )


# ---------------------------------------------------------------------------
# GET /api/git/branches
# ---------------------------------------------------------------------------


@router.get("/branches", response_model=GitBranchListResponse)
async def git_branches() -> GitBranchListResponse:
    """List all branches (user's first, then others, archived last)."""
    try:
        result = list_branches()
    except GitError as e:
        _handle_git_error(e)
    return GitBranchListResponse(
        current=result.current,
        branches=[
            GitBranchItem(
                name=b.name,
                is_yours=b.is_yours,
                is_current=b.is_current,
                is_archived=b.is_archived,
                last_commit_time=b.last_commit_time,
                commit_count=b.commit_count,
            )
            for b in result.branches
        ],
    )


# ---------------------------------------------------------------------------
# POST /api/git/branches — create a new branch
# ---------------------------------------------------------------------------


@router.post("/branches", response_model=GitCreateBranchResponse)
async def git_create_branch(body: GitCreateBranchRequest) -> GitCreateBranchResponse:
    """Create a new branch from current HEAD."""
    if not body.description.strip():
        raise HTTPException(status_code=400, detail="Branch description cannot be empty.")
    try:
        branch = create_branch(body.description)
    except GitError as e:
        _handle_git_error(e)
    return GitCreateBranchResponse(branch=branch)


# ---------------------------------------------------------------------------
# POST /api/git/switch
# ---------------------------------------------------------------------------


@router.post("/switch")
async def git_switch(body: GitSwitchBranchRequest) -> dict[str, str]:
    """Switch to a branch (auto-commits pending changes first)."""
    try:
        switch_branch(body.branch)
    except GitError as e:
        _handle_git_error(e)
    return {"status": "ok", "branch": body.branch}


# ---------------------------------------------------------------------------
# POST /api/git/save
# ---------------------------------------------------------------------------


@router.post("/save", response_model=GitSaveResponse)
async def git_save() -> GitSaveResponse:
    """Stage, commit, and push all changes."""
    try:
        result = save_progress()
    except GitError as e:
        _handle_git_error(e)
    return GitSaveResponse(
        commit_sha=result.commit_sha,
        message=result.message,
        timestamp=result.timestamp,
    )


# ---------------------------------------------------------------------------
# POST /api/git/submit
# ---------------------------------------------------------------------------


@router.post("/submit", response_model=GitSubmitResponse)
async def git_submit() -> GitSubmitResponse:
    """Push and return a comparison URL for PR creation."""
    try:
        result = submit_for_review()
    except GitError as e:
        _handle_git_error(e)
    return GitSubmitResponse(
        compare_url=result.compare_url,
        branch=result.branch,
    )


# ---------------------------------------------------------------------------
# GET /api/git/history
# ---------------------------------------------------------------------------


@router.get("/history", response_model=GitHistoryResponse)
async def git_history(limit: int = 20) -> GitHistoryResponse:
    """Commit history for the current branch."""
    try:
        entries = get_history(limit=limit)
    except GitError as e:
        _handle_git_error(e)
    return GitHistoryResponse(
        entries=[
            GitHistoryEntry(
                sha=e.sha,
                short_sha=e.short_sha,
                message=e.message,
                timestamp=e.timestamp,
                files_changed=e.files_changed,
            )
            for e in entries
        ],
    )


# ---------------------------------------------------------------------------
# POST /api/git/revert
# ---------------------------------------------------------------------------


@router.post("/revert", response_model=GitRevertResponse)
async def git_revert(body: GitRevertRequest) -> GitRevertResponse:
    """Reset to a specific commit (creates a backup tag first)."""
    try:
        result = revert_to(body.sha)
    except GitError as e:
        _handle_git_error(e)
    return GitRevertResponse(
        backup_tag=result.backup_tag,
        reverted_to=result.reverted_to,
    )


# ---------------------------------------------------------------------------
# POST /api/git/pull
# ---------------------------------------------------------------------------


@router.post("/pull", response_model=GitPullResponse)
async def git_pull() -> GitPullResponse:
    """Pull latest default branch into current branch."""
    try:
        result = pull_latest()
    except GitError as e:
        _handle_git_error(e)
    return GitPullResponse(
        success=result.success,
        conflict=result.conflict,
        conflict_message=result.conflict_message,
        commits_pulled=result.commits_pulled,
    )


# ---------------------------------------------------------------------------
# POST /api/git/archive
# ---------------------------------------------------------------------------


@router.post("/archive", response_model=GitArchiveResponse)
async def git_archive(body: GitArchiveRequest) -> GitArchiveResponse:
    """Archive a branch (rename to archive/<name>)."""
    try:
        archived_as = archive_branch(body.branch)
    except GitError as e:
        _handle_git_error(e)
    return GitArchiveResponse(archived_as=archived_as)


# ---------------------------------------------------------------------------
# DELETE /api/git/branches
# ---------------------------------------------------------------------------


@router.delete("/branches")
async def git_delete_branch(body: GitDeleteBranchRequest) -> dict[str, str]:
    """Permanently delete a branch."""
    try:
        delete_branch(body.branch)
    except GitError as e:
        _handle_git_error(e)
    return {"status": "ok", "branch": body.branch}
