"""
Tool C: Read File - Read full content of a file from HDFS
"""
import subprocess
from typing import Any, Optional
import google.generativeai as genai

from ..config import Config
from .query_hive import query_hive

def read_hdfs_file_content(path: str) -> str:
    """Read a text file from HDFS using docker exec."""
    try:
        # Check if file exists first to avoid cat error noise
        check_cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-test", "-e", path]
        if subprocess.run(check_cmd).returncode != 0:
            return f"Error: File not found at {path}"

        cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-cat", path]
        print(f"       ⚙️ HDFS Command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode != 0:
            raise Exception(f"HDFS Error: {result.stderr}")
            
        return result.stdout
    except subprocess.TimeoutExpired:
        raise Exception("Timeout reading file from HDFS")
    except Exception as e:
        return f"Error reading file: {str(e)}"


def get_file_content(
    file_path: str,
    repo_name: str = "PIVO",
    commit_hash: Optional[str] = None,
    config: Optional[Config] = None
) -> dict[str, Any]:
    """
    Get the content of a specific file from HDFS.
    
    Args:
        file_path: Relative path of the file (e.g. 'docker-compose.yml')
        repo_name: Repository name (defaults to PIVO)
        commit_hash: Specific commit (defaults to latest if None)
    """
    if not config:
        config = Config.from_env()

    # If commit hash is not provided, find the latest one
    if not commit_hash:
        print(f"[TOOL] Resolving latest commit for {repo_name}...")
        # We can use the SQLite helper directly or the tool
        import sqlite3
        try:
            conn = sqlite3.connect("pivo.db")
            cursor = conn.execute(
                "SELECT commit_hash, hdfs_path FROM repo_snapshots WHERE repo_name = ? ORDER BY commit_timestamp DESC LIMIT 1",
                (repo_name,)
            )
            row = cursor.fetchone()
            conn.close()
            
            if row:
                commit_hash = row[0]
                base_hdfs_path = row[1]
            else:
                return {"success": False, "error": f"No backups found for repo '{repo_name}'"}
                
        except Exception as e:
             return {"success": False, "error": f"Metadata resolution failed: {e}"}
    else:
        # We need the base hdfs path for this commit
        import sqlite3
        try:
            conn = sqlite3.connect("pivo.db")
            cursor = conn.execute(
                "SELECT hdfs_path FROM repo_snapshots WHERE commit_hash = ?",
                (commit_hash,)
            )
            row = cursor.fetchone()
            conn.close()
            
            if row:
                base_hdfs_path = row[0]
            else:
                 return {"success": False, "error": f"Commit '{commit_hash}' not found in catalog"}
        except Exception:
             return {"success": False, "error": "Database error"}

    # Construct full HDFS path
    # base_path usually looks like /backups/PIVO/hash
    # file_path might be 'docker-compose.yml' or 'pivo/config.py'
    
    full_path = f"{base_hdfs_path}/{file_path}"
    
    print(f"[TOOL] Reading {full_path}...")
    content = read_hdfs_file_content(full_path)
    
    if content.startswith("Error"):
        return {"success": False, "error": content}
        
    # Truncate for LLM context limits
    MAX_CHARS = 20000
    if len(content) > MAX_CHARS:
        content = content[:MAX_CHARS] + "\n...[TRUNCATED due to length]..."
    
    return {
        "success": True,
        "repo": repo_name,
        "commit": commit_hash,
        "path": file_path,
        "content": content
    }
