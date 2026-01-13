"""
Hive Cataloger - Manage repo_snapshots table and insert metadata
Uses docker exec + beeline for Hive 4.x compatibility
"""
import subprocess
from typing import Optional

from ..config import Config
from .github_cloner import CommitMetadata


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS repo_snapshots (
    commit_hash     STRING,
    repo_name       STRING,
    author          STRING,
    author_email    STRING,
    commit_message  STRING,
    commit_timestamp STRING,
    files_changed   ARRAY<STRING>,
    additions       INT,
    deletions       INT,
    branch          STRING,
    hdfs_path       STRING
)
STORED AS ORC
"""


def run_hive_query(query: str, fetch: bool = False) -> tuple[bool, list[str]]:
    """
    Run a Hive query via docker exec + beeline.
    
    Args:
        query: HiveQL query to execute
        fetch: Whether to return results
    
    Returns:
        (success, result_lines)
    """
    # Escape the query for shell
    escaped_query = query.replace('"', '\\"').replace("'", "\\'")
    
    cmd = [
        "docker", "exec", "hive",
        "beeline", "-u", "jdbc:hive2://localhost:10000",
        "--silent=true",
        "--outputformat=csv2",
        "-e", query
    ]
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            lines = [l.strip() for l in result.stdout.strip().split('\n') if l.strip()]
            return True, lines
        else:
            # Check if it's just a warning
            if "SLF4J" in result.stderr and not "Error" in result.stderr:
                return True, []
            return False, [result.stderr]
    except subprocess.TimeoutExpired:
        return False, ["Query timed out"]
    except Exception as e:
        return False, [str(e)]


def create_table(config: Config) -> bool:
    """
    Create the repo_snapshots table if it doesn't exist.
    """
    success, output = run_hive_query(CREATE_TABLE_SQL)
    
    if success:
        print("[INFO] repo_snapshots table ready")
    else:
        print(f"[WARN] Table creation: {output}")
    
    return success


def insert_snapshot(
    metadata: CommitMetadata,
    hdfs_path: str,
    config: Config
) -> bool:
    """
    Insert a snapshot metadata row into Hive.
    """
    # Escape special characters for HiveQL
    def escape(s: str) -> str:
        return s.replace("'", "''").replace("\\", "\\\\").replace("\n", " ")[:200]
    
    # Format files array for Hive
    files_array = "ARRAY(" + ", ".join([f"'{escape(f)}'" for f in metadata.files_changed[:50]]) + ")" if metadata.files_changed else "ARRAY()"
    
    insert_sql = f"""
    INSERT INTO repo_snapshots 
    SELECT 
        '{metadata.commit_hash}',
        '{escape(metadata.repo_name)}',
        '{escape(metadata.author)}',
        '{escape(metadata.author_email)}',
        '{escape(metadata.commit_message)}',
        '{escape(metadata.commit_timestamp)}',
        {files_array},
        {metadata.additions},
        {metadata.deletions},
        '{escape(metadata.branch)}',
        '{hdfs_path}'
    """
    
    success, output = run_hive_query(insert_sql)
    
    if success:
        print(f"[INFO] Cataloged commit {metadata.commit_hash[:7]} in Hive")
    else:
        print(f"[ERROR] Failed to insert: {output}")
    
    return success


def check_snapshot_exists(commit_hash: str, config: Config) -> bool:
    """
    Check if a snapshot already exists in Hive.
    """
    query = f"SELECT COUNT(*) FROM repo_snapshots WHERE commit_hash = '{commit_hash}'"
    success, output = run_hive_query(query, fetch=True)
    
    if success and output:
        try:
            # Skip header if present
            for line in output:
                if line.isdigit():
                    return int(line) > 0
        except Exception:
            pass
    
    return False


def get_all_snapshots(config: Config) -> list[dict]:
    """
    Get all snapshots from Hive.
    """
    query = "SELECT commit_hash, repo_name, author, commit_message, hdfs_path FROM repo_snapshots"
    success, output = run_hive_query(query, fetch=True)
    
    results = []
    if success and output:
        # Parse CSV output
        headers = None
        for line in output:
            if not headers:
                headers = line.split(',')
                continue
            values = line.split(',')
            if len(values) == len(headers):
                results.append(dict(zip(headers, values)))
    
    return results
