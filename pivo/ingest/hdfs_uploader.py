"""
HDFS Uploader - Write repository files to HDFS via WebHDFS API
"""
import os
from pathlib import Path
from hdfs import InsecureClient

from ..config import Config


def upload_to_hdfs(
    local_path: Path,
    repo_name: str,
    commit_hash: str,
    config: Config
) -> str:
    """
    Upload a local directory tree to HDFS.
    
    Args:
        local_path: Path to local repository
        repo_name: Name of the repository
        commit_hash: Commit hash for path organization
        config: PIVO configuration
    
    Returns:
        HDFS path where files were uploaded
    """
    hdfs_base_path = f"/backups/{repo_name}/{commit_hash}"
    
    # Connect to HDFS via WebHDFS
    client = InsecureClient(config.hdfs_url, user='root')
    
    # Create base directory
    client.makedirs(hdfs_base_path)
    
    # Walk through local directory and upload files
    files_uploaded = 0
    for root, dirs, files in os.walk(local_path):
        # Skip .git directory
        if ".git" in root:
            continue
        
        for filename in files:
            local_file = Path(root) / filename
            
            # Calculate relative path
            rel_path = local_file.relative_to(local_path)
            hdfs_file_path = f"{hdfs_base_path}/{rel_path}"
            
            # Create parent directories in HDFS
            hdfs_parent = str(Path(hdfs_file_path).parent)
            try:
                client.makedirs(hdfs_parent)
            except Exception:
                pass  # Directory may already exist
            
            # Upload file
            try:
                with open(local_file, 'rb') as f:
                    client.write(hdfs_file_path, f, overwrite=True)
                files_uploaded += 1
            except Exception as e:
                print(f"[WARN] Failed to upload {rel_path}: {e}")
    
    print(f"[INFO] Uploaded {files_uploaded} files to {hdfs_base_path}")
    return hdfs_base_path


def list_hdfs_backups(config: Config) -> list[dict]:
    """
    List all repository backups in HDFS.
    
    Returns:
        List of backup info (repo_name, commit_hash, path)
    """
    client = InsecureClient(config.hdfs_url, user='root')
    
    backups = []
    try:
        repos = client.list("/backups")
        for repo in repos:
            commits = client.list(f"/backups/{repo}")
            for commit in commits:
                backups.append({
                    "repo_name": repo,
                    "commit_hash": commit,
                    "hdfs_path": f"/backups/{repo}/{commit}"
                })
    except Exception as e:
        print(f"[WARN] Could not list backups: {e}")
    
    return backups


def delete_backup(repo_name: str, commit_hash: str, config: Config) -> bool:
    """
    Delete a specific backup from HDFS.
    
    Args:
        repo_name: Repository name
        commit_hash: Commit hash
        config: PIVO configuration
    
    Returns:
        True if deleted successfully
    """
    client = InsecureClient(config.hdfs_url, user='root')
    hdfs_path = f"/backups/{repo_name}/{commit_hash}"
    
    try:
        client.delete(hdfs_path, recursive=True)
        print(f"[INFO] Deleted {hdfs_path}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to delete: {e}")
        return False
