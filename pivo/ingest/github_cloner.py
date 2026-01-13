"""
GitHub Cloner - Clone repos and extract commit metadata
"""
import os
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


@dataclass
class CommitMetadata:
    """Metadata extracted from a git commit."""
    commit_hash: str
    repo_name: str
    author: str
    author_email: str
    commit_message: str
    commit_timestamp: str
    files_changed: list[str]
    additions: int
    deletions: int
    branch: str
    local_path: Path


def clone_repo(
    repo_url: str,
    commit_hash: Optional[str] = None,
    github_token: Optional[str] = None,
    target_dir: Optional[Path] = None
) -> Path:
    """
    Clone a GitHub repository to a local directory.
    
    Args:
        repo_url: GitHub repository URL
        commit_hash: Specific commit to checkout (default: HEAD)
        github_token: Optional token for private repos
        target_dir: Where to clone (default: temp directory)
    
    Returns:
        Path to cloned repository
    """
    # Create target directory
    if target_dir is None:
        target_dir = Path(tempfile.mkdtemp(prefix="pivo_clone_"))
    
    # Build authenticated URL if token provided
    clone_url = repo_url
    if github_token and repo_url.startswith("https://github.com/"):
        clone_url = repo_url.replace(
            "https://github.com/",
            f"https://{github_token}@github.com/"
        )
    
    # Clone repository
    result = subprocess.run(
        ["git", "clone", "--depth", "50", clone_url, str(target_dir)],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise RuntimeError(f"Git clone failed: {result.stderr}")
    
    # Checkout specific commit if provided
    if commit_hash:
        # Fetch key commit directly to ensure we have it (handling non-default branches)
        # Note: GitHub allows fetching by sha1
        subprocess.run(
            ["git", "fetch", "origin", commit_hash],
            cwd=target_dir,
            capture_output=True
        )

        result = subprocess.run(
            ["git", "checkout", commit_hash],
            cwd=target_dir,
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Git checkout failed: {result.stderr}")
    
    return target_dir


def get_commit_metadata(repo_path: Path, repo_url: str) -> CommitMetadata:
    """
    Extract metadata from the current commit in a git repository.
    
    Args:
        repo_path: Path to the cloned repository
        repo_url: Original repository URL (for naming)
    
    Returns:
        CommitMetadata with all extracted information
    """
    def git_cmd(args: list[str]) -> str:
        result = subprocess.run(
            ["git"] + args,
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        return result.stdout.strip()
    
    # Extract repo name from URL
    repo_name = repo_url.rstrip("/").split("/")[-1]
    if repo_name.endswith(".git"):
        repo_name = repo_name[:-4]
    
    # Get commit info
    commit_hash = git_cmd(["rev-parse", "HEAD"])
    author = git_cmd(["log", "-1", "--format=%an"])
    author_email = git_cmd(["log", "-1", "--format=%ae"])
    commit_message = git_cmd(["log", "-1", "--format=%s"])
    commit_timestamp = git_cmd(["log", "-1", "--format=%ci"])
    
    # Get current branch
    branch = git_cmd(["rev-parse", "--abbrev-ref", "HEAD"])
    if branch == "HEAD":  # Detached HEAD
        branch = "detached"
    
    # Get files changed in this commit
    files_output = git_cmd(["diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"])
    files_changed = [f for f in files_output.split("\n") if f]
    
    # Get additions/deletions stats
    stats_output = git_cmd(["diff-tree", "--no-commit-id", "--numstat", "-r", "HEAD"])
    additions = 0
    deletions = 0
    for line in stats_output.split("\n"):
        if line:
            parts = line.split("\t")
            if len(parts) >= 2:
                try:
                    additions += int(parts[0]) if parts[0] != "-" else 0
                    deletions += int(parts[1]) if parts[1] != "-" else 0
                except ValueError:
                    pass
    
    return CommitMetadata(
        commit_hash=commit_hash,
        repo_name=repo_name,
        author=author,
        author_email=author_email,
        commit_message=commit_message,
        commit_timestamp=commit_timestamp,
        files_changed=files_changed,
        additions=additions,
        deletions=deletions,
        branch=branch,
        local_path=repo_path
    )


def get_recent_commits(repo_path: Path, repo_url: str, count: int = 10) -> list[CommitMetadata]:
    """
    Get metadata for the most recent N commits.
    
    Args:
        repo_path: Path to the cloned repository
        repo_url: Original repository URL
        count: Number of commits to retrieve
    
    Returns:
        List of CommitMetadata objects
    """
    result = subprocess.run(
        ["git", "log", f"-{count}", "--format=%H"],
        cwd=repo_path,
        capture_output=True,
        text=True
    )
    
    commits = []
    for commit_hash in result.stdout.strip().split("\n"):
        if commit_hash:
            # Checkout each commit and get metadata
            subprocess.run(
                ["git", "checkout", commit_hash],
                cwd=repo_path,
                capture_output=True
            )
            metadata = get_commit_metadata(repo_path, repo_url)
            commits.append(metadata)
    
    return commits
