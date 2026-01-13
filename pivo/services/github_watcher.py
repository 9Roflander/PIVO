"""
GitHub Watcher Service - PIVO
-----------------------------
Periodically checks tracked repositories for new commits across ALL branches.
If a new commit is found (not in pivo.db), it triggers 'ingest.py'.
"""
import time
import subprocess
import sqlite3
import sys
from pathlib import Path
from typing import List, Dict
from pivo.ingest.hive_cataloger import get_tracked_repos_from_db

# Config
POLL_INTERVAL_SECONDS = 60
# Assuming run from project root, pivo.db is in root.
DB_PATH = Path("pivo.db").absolute()

def get_tracked_repos() -> List[str]:
    """
    Get list of unique repo URLs from the database.
    """
    db_repos = get_tracked_repos_from_db()
    
    # Always include the known PIVO repo if not present
    default_repo = "https://github.com/9Roflander/PIVO"
    if default_repo not in db_repos:
        db_repos.append(default_repo)
        
    return db_repos

def get_remote_branches(repo_url: str) -> Dict[str, str]:
    """Get dictionary of {branch_name: commit_hash} for all remote heads."""
    branches = {}
    try:
        # List all heads
        cmd = ["git", "ls-remote", "--heads", repo_url]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0 and result.stdout:
            for line in result.stdout.strip().split('\n'):
                if not line: continue
                parts = line.split()
                if len(parts) == 2:
                    commit_hash, ref = parts
                    # ref is roughly "refs/heads/branchname"
                    branch_name = ref.replace("refs/heads/", "")
                    branches[branch_name] = commit_hash
    except Exception as e:
        print(f"[WATCHER] Error checking {repo_url}: {e}")
    return branches

def commit_exists(commit_hash: str) -> bool:
    """Check if commit exists in local SQLite DB."""
    if not DB_PATH.exists():
        return False
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute(
            "SELECT 1 FROM repo_snapshots WHERE commit_hash = ?", 
            (commit_hash,)
        )
        exists = cursor.fetchone() is not None
        conn.close()
        return exists
    except Exception as e:
        print(f"[WATCHER] DB Error: {e}")
        return False

def trigger_ingestion(repo_url: str, commit_hash: str, branch: str):
    """Run ingestion for the detected repo/commit using pivo.ingest.cli module."""
    print(f"[WATCHER] 🚀 Triggering ingestion for {repo_url} ({branch})...")
    try:
        # Call the ingest module via python -m pivo.ingest.cli
        cmd = [sys.executable, "-m", "pivo.ingest.cli", "--repo", repo_url, "--commit", commit_hash]
        subprocess.run(cmd, check=True)
        print(f"[WATCHER] ✅ Ingestion complete for {branch}.")
    except subprocess.CalledProcessError as e:
        print(f"[WATCHER] ❌ Ingestion aborted: {e}")

def main():
    print(f"[*] Starting GitHub Watcher (Interval: {POLL_INTERVAL_SECONDS}s, Mode: Multi-Branch)...")
    
    while True:
        repos = get_tracked_repos()
        for repo in repos:
            print(f"[WATCHER] Scanning branches for {repo}...")
            branch_map = get_remote_branches(repo)
            
            if not branch_map:
                print(f"[WATCHER] No branches found or error accessing repo.")
                continue
                
            new_commits_found = 0
            for branch, commit_hash in branch_map.items():
                if not commit_exists(commit_hash):
                    print(f"[WATCHER] 🆕 New commit on branch '{branch}': {commit_hash[:7]}")
                    trigger_ingestion(repo, commit_hash, branch)
                    new_commits_found += 1
            
            if new_commits_found == 0:
                print(f"[WATCHER] ALL {len(branch_map)} branches up to date.")
        
        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
