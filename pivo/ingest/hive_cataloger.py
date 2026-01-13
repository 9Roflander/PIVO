"""
Hive Cataloger - Manage repo_snapshots table and insert metadata
(Implementation switched to SQLite for reliability on ARM Macs)
"""
import sqlite3
import json
from pathlib import Path
from typing import Optional

from ..config import Config
from .github_cloner import CommitMetadata


def get_db_path() -> str:
    """Get path to local SQLite database."""
    # Store in the project root
    db_path = Path("pivo.db").absolute()
    return str(db_path)


def get_connection():
    """Get SQLite connection."""
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def create_table(config: Config) -> bool:
    """
    Create the repo_snapshots table if it doesn't exist.
    """
    create_sql = """
    CREATE TABLE IF NOT EXISTS repo_snapshots (
        commit_hash     TEXT PRIMARY KEY,
        repo_name       TEXT,
        author          TEXT,
        author_email    TEXT,
        commit_message  TEXT,
        commit_timestamp TEXT,
        files_changed   TEXT, -- JSON array
        additions       INTEGER,
        deletions       INTEGER,
        branch          TEXT,
        hdfs_path       TEXT
    )
    """
    try:
        with get_connection() as conn:
            conn.execute(create_sql)
        print("[INFO] Metadata catalog ready (SQLite)")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to create table: {e}")
        return False


def insert_snapshot(
    metadata: CommitMetadata,
    hdfs_path: str,
    config: Config
) -> bool:
    """
    Insert a snapshot metadata row into SQLite.
    """
    insert_sql = """
    INSERT OR REPLACE INTO repo_snapshots (
        commit_hash, repo_name, author, author_email, 
        commit_message, commit_timestamp, files_changed, 
        additions, deletions, branch, hdfs_path
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    try:
        # Serialize files list to JSON
        files_json = json.dumps(metadata.files_changed) if metadata.files_changed else "[]"
        
        with get_connection() as conn:
            conn.execute(insert_sql, (
                metadata.commit_hash,
                metadata.repo_name,
                metadata.author,
                metadata.author_email,
                metadata.commit_message,
                metadata.commit_timestamp,
                files_json,
                metadata.additions,
                metadata.deletions,
                metadata.branch,
                hdfs_path
            ))
        print(f"[INFO] Cataloged commit {metadata.commit_hash[:7]} in Metadata Store")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to insert: {e}")
        return False


def check_snapshot_exists(commit_hash: str, config: Config) -> bool:
    """
    Check if a snapshot already exists.
    """
    try:
        with get_connection() as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM repo_snapshots WHERE commit_hash = ?", 
                (commit_hash,)
            )
            return cursor.fetchone()[0] > 0
    except Exception:
        return False


def get_all_snapshots(config: Config) -> list[dict]:
    """
    Get all snapshots.
    """
    try:
        with get_connection() as conn:
            cursor = conn.execute("SELECT * FROM repo_snapshots")
            return [dict(row) for row in cursor.fetchall()]
    except Exception:
        return []
