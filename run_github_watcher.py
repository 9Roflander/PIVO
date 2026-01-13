"""
GitHub Watcher Service - PIVO
-----------------------------
Periodically checks tracked repositories for new commits.
If a new commit is found (not in pivo.db), it triggers 'ingest.py'.
"""
import time
import subprocess
import sqlite3
import sys
from pathlib import Path
from typing import List, Dict

# Config
POLL_INTERVAL_SECONDS = 60
DB_PATH = Path("pivo.db").absolute()

def get_tracked_repos() -> List[str]:
    """
    Get list of unique repo URLs from the database.
    In a real app, we'd have a specific table for 'tracked_repos'.
    For now, we infer from existing snapshots or default to PIVO repo.
    """
    # Default to the known PIVO repo for this demo
    # Ideally, we read from a config or arguments
    return ["https://github.com/9Roflander/PIVO"]

def get_latest_remote_hash(repo_url: str) -> str:
    """Get the latest commit hash from the remote repository."""
    try:
        cmd = ["git", "ls-remote", repo_url, "HEAD"]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0 and result.stdout:
            # Output format: <hash>\tHEAD
            return result.stdout.split()[0]
    except Exception as e:
        print(f"[WATCHER] Error checking {repo_url}: {e}")
    return None

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

def trigger_ingestion(repo_url: str):
    """Run ingest.py for the detected repo."""
    print(f"[WATCHER] 🚀 Triggering ingestion for {repo_url}...")
    try:
        # Run ingest.py in a subprocess
        cmd = [sys.executable, "ingest.py", "--repo", repo_url]
        subprocess.run(cmd, check=True)
        print(f"[WATCHER] ✅ Ingestion complete.")
    except subprocess.CalledProcessError as e:
        print(f"[WATCHER] ❌ Ingestion aborted: {e}")

def main():
    print(f"[*] Starting GitHub Watcher (Interval: {POLL_INTERVAL_SECONDS}s)...")
    
    while True:
        repos = get_tracked_repos()
        for repo in repos:
            print(f"[WATCHER] Checking {repo}...")
            remote_hash = get_latest_remote_hash(repo)
            
            if remote_hash:
                if not commit_exists(remote_hash):
                    print(f"[WATCHER] 🆕 New commit detected: {remote_hash[:7]}")
                    trigger_ingestion(repo)
                else:
                    print(f"[WATCHER] No new changes (HEAD: {remote_hash[:7]})")
            else:
                 print(f"[WATCHER] Could not resolve HEAD.")
        
        time.sleep(POLL_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
