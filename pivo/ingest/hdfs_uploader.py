"""
HDFS Uploader - Write repository files to HDFS via Docker exec
Uses docker exec to bypass WebHDFS datanode resolution issues from host machine.
"""
import os
import subprocess
import tarfile
import tempfile
from pathlib import Path

from ..config import Config


def upload_to_hdfs(
    local_path: Path,
    repo_name: str,
    commit_hash: str,
    config: Config
) -> str:
    """
    Upload a local directory tree to HDFS using docker exec.
    
    Args:
        local_path: Path to local repository
        repo_name: Name of the repository
        commit_hash: Commit hash for path organization
        config: PIVO configuration
    
    Returns:
        HDFS path where files were uploaded
    """
    hdfs_base_path = f"/backups/{repo_name}/{commit_hash}"
    
    # Create tar archive of the repository (excluding .git)
    with tempfile.NamedTemporaryFile(suffix='.tar', delete=False) as tmp_tar:
        tar_path = tmp_tar.name
    
    try:
        # Create tar archive
        with tarfile.open(tar_path, "w") as tar:
            for item in os.listdir(local_path):
                if item == ".git":
                    continue
                item_path = local_path / item
                tar.add(item_path, arcname=item)
        
        # Copy tar to namenode container
        subprocess.run(
            ["docker", "cp", tar_path, "namenode:/tmp/repo.tar"],
            check=True,
            capture_output=True
        )
        
        # Create HDFS directory
        subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-mkdir", "-p", hdfs_base_path],
            check=True,
            capture_output=True
        )
        
        # Extract and upload to HDFS
        extract_and_upload_cmd = f"""
            cd /tmp && \
            rm -rf repo_extract && \
            mkdir -p repo_extract && \
            tar -xf repo.tar -C repo_extract && \
            hdfs dfs -put -f repo_extract/* {hdfs_base_path}/ && \
            rm -rf repo_extract repo.tar
        """
        
        result = subprocess.run(
            ["docker", "exec", "namenode", "bash", "-c", extract_and_upload_cmd],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"[WARN] HDFS upload warning: {result.stderr}")
        
        # Count files uploaded
        count_result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-count", hdfs_base_path],
            capture_output=True,
            text=True
        )
        
        if count_result.returncode == 0:
            parts = count_result.stdout.strip().split()
            if len(parts) >= 2:
                file_count = parts[1]
                print(f"[INFO] Uploaded {file_count} files to {hdfs_base_path}")
        else:
            print(f"[INFO] Uploaded files to {hdfs_base_path}")
        
        return hdfs_base_path
        
    finally:
        # Cleanup temp tar file
        if os.path.exists(tar_path):
            os.unlink(tar_path)


def list_hdfs_backups(config: Config) -> list[dict]:
    """
    List all repository backups in HDFS.
    
    Returns:
        List of backup info (repo_name, commit_hash, path)
    """
    backups = []
    
    try:
        # List /backups directory
        result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", "/backups"],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            return backups
        
        for line in result.stdout.strip().split("\n"):
            if line.startswith("d"):
                parts = line.split()
                if parts:
                    repo_path = parts[-1]
                    repo_name = repo_path.split("/")[-1]
                    
                    # List commits for this repo
                    commits_result = subprocess.run(
                        ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", repo_path],
                        capture_output=True,
                        text=True
                    )
                    
                    for commit_line in commits_result.stdout.strip().split("\n"):
                        if commit_line.startswith("d"):
                            commit_parts = commit_line.split()
                            if commit_parts:
                                commit_path = commit_parts[-1]
                                commit_hash = commit_path.split("/")[-1]
                                backups.append({
                                    "repo_name": repo_name,
                                    "commit_hash": commit_hash,
                                    "hdfs_path": commit_path
                                })
    except Exception as e:
        print(f"[WARN] Could not list backups: {e}")
    
    return backups


def delete_backup(repo_name: str, commit_hash: str, config: Config) -> bool:
    """
    Delete a specific backup from HDFS.
    """
    hdfs_path = f"/backups/{repo_name}/{commit_hash}"
    
    try:
        result = subprocess.run(
            ["docker", "exec", "namenode", "hdfs", "dfs", "-rm", "-r", hdfs_path],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print(f"[INFO] Deleted {hdfs_path}")
            return True
        else:
            print(f"[ERROR] Failed to delete: {result.stderr}")
            return False
    except Exception as e:
        print(f"[ERROR] Failed to delete: {e}")
        return False
