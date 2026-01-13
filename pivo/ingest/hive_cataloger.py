"""
Hive Cataloger - Manage repo_snapshots table and insert metadata
"""
from pyhive import hive
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
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
COLLECTION ITEMS TERMINATED BY '|'
STORED AS TEXTFILE
"""


def get_hive_connection(config: Config):
    """Get a connection to Hive."""
    return hive.connect(
        host=config.hive_host,
        port=config.hive_port,
        username="hive"
    )


def create_table(config: Config) -> bool:
    """
    Create the repo_snapshots table if it doesn't exist.
    
    Args:
        config: PIVO configuration
    
    Returns:
        True if table was created/exists
    """
    try:
        conn = get_hive_connection(config)
        cursor = conn.cursor()
        cursor.execute(CREATE_TABLE_SQL)
        cursor.close()
        conn.close()
        print("[INFO] repo_snapshots table ready")
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
    Insert a snapshot metadata row into Hive.
    
    Args:
        metadata: Commit metadata from cloner
        hdfs_path: HDFS path where files are stored
        config: PIVO configuration
    
    Returns:
        True if inserted successfully
    """
    # Escape single quotes in strings
    def escape(s: str) -> str:
        return s.replace("'", "\\'").replace("\n", " ")
    
    # Format files array for Hive
    files_str = "|".join(metadata.files_changed) if metadata.files_changed else ""
    
    insert_sql = f"""
    INSERT INTO repo_snapshots VALUES (
        '{metadata.commit_hash}',
        '{escape(metadata.repo_name)}',
        '{escape(metadata.author)}',
        '{escape(metadata.author_email)}',
        '{escape(metadata.commit_message)}',
        '{escape(metadata.commit_timestamp)}',
        ARRAY({', '.join([f"'{escape(f)}'" for f in metadata.files_changed]) if metadata.files_changed else ''}),
        {metadata.additions},
        {metadata.deletions},
        '{escape(metadata.branch)}',
        '{hdfs_path}'
    )
    """
    
    try:
        conn = get_hive_connection(config)
        cursor = conn.cursor()
        cursor.execute(insert_sql)
        cursor.close()
        conn.close()
        print(f"[INFO] Cataloged commit {metadata.commit_hash[:7]} in Hive")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to insert: {e}")
        return False


def check_snapshot_exists(commit_hash: str, config: Config) -> bool:
    """
    Check if a snapshot already exists in Hive.
    
    Args:
        commit_hash: Commit hash to check
        config: PIVO configuration
    
    Returns:
        True if snapshot already cataloged
    """
    try:
        conn = get_hive_connection(config)
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COUNT(*) FROM repo_snapshots WHERE commit_hash = '{commit_hash}'"
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] > 0 if result else False
    except Exception:
        return False


def get_all_snapshots(config: Config) -> list[dict]:
    """
    Get all snapshots from Hive.
    
    Returns:
        List of snapshot dictionaries
    """
    try:
        conn = get_hive_connection(config)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM repo_snapshots")
        
        columns = [desc[0] for desc in cursor.description]
        results = []
        for row in cursor.fetchall():
            results.append(dict(zip(columns, row)))
        
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        print(f"[ERROR] Failed to query: {e}")
        return []
