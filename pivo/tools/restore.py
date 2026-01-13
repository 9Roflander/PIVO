"""
Tool C: Restore - Trigger Spark job to restore repository to a specific commit
"""
import os
import subprocess
import sqlite3
from typing import Any
from pathlib import Path

from ..config import Config


# Claude Tool Definition
RESTORE_TOOL = {
    "name": "submit_restore_job",
    "description": "Restore a repository to a specific commit state and push it to a new GitHub location. This triggers a Spark job that reads the snapshot from HDFS and pushes to the target repository. CRITICAL: The target GitHub repository MUST be completely empty (no branches/tags), otherwise the operation will fail.",
    "input_schema": {
        "type": "object",
        "properties": {
            "commit_hash": {
                "type": "string",
                "description": "The git commit hash to restore to"
            },
            "repo_name": {
                "type": "string",
                "description": "Name of the repository to restore"
            },
            "target_repo_url": {
                "type": "string",
                "description": "GitHub URL where the restored repo should be pushed (e.g., https://github.com/user/new-repo)"
            },
            "github_api_key": {
                "type": "string",
                "description": "GitHub Personal Access Token with repo write permissions"
            }
        },
        "required": ["commit_hash", "repo_name", "target_repo_url", "github_api_key"]
    }
}


def submit_restore_job(
    commit_hash: str,
    repo_name: str,
    target_repo_url: str,
    github_api_key: str,
    config: Config
) -> dict[str, Any]:
    """
    Submit a Spark job to restore a repository snapshot.
    
    1. Validate inputs
    2. Launch spark-submit as subprocess
    3. Pass API key securely via environment variable
    4. Return job status
    
    SECURITY: The github_api_key is NEVER logged or saved to disk.
    """
    # Validate commit hash format
    if not commit_hash or len(commit_hash) < 7:
        return {
            "success": False,
            "error": "Invalid commit hash. Must be at least 7 characters."
        }
    
    # NEW: Resolve short hash to full hash via SQLite
    full_hash = commit_hash
    if len(commit_hash) < 40:
        try:
            db_path = Path(__file__).parent.parent.parent / "pivo.db"
            if db_path.exists():
                conn = sqlite3.connect(db_path)
                cursor = conn.execute(
                    "SELECT commit_hash FROM repo_snapshots WHERE repo_name = ? AND commit_hash LIKE ?",
                    (repo_name, f"{commit_hash}%")
                )
                results = cursor.fetchall()
                conn.close()
                
                if not results:
                    return {"success": False, "error": f"No backup found with commit hash starting with '{commit_hash}' for repository '{repo_name}'."}
                if len(results) > 1:
                    similar = ", ".join([r[0][:10] for r in results[:3]])
                    return {"success": False, "error": f"Ambiguous commit hash '{commit_hash}'. Found multiple matches: {similar}..."}
                
                full_hash = results[0][0]
                print(f"       ✅ Resolved {commit_hash} -> {full_hash}")
        except Exception as e:
            print(f"       ⚠️ Hash resolution error: {e}")
            # Fallback to original hash if DB fails
    
    # Validate target URL
    if not target_repo_url.startswith(("https://github.com/", "git@github.com:")):
        return {
            "success": False,
            "error": "Target URL must be a valid GitHub repository URL"
        }
    
    # Path to Spark job script
    project_root = Path(__file__).parent.parent.parent
    spark_job_path = project_root / "spark_jobs" / "restore_job.py"
    
    if not spark_job_path.exists():
        return {
            "success": False,
            "error": f"Spark job script not found at {spark_job_path}"
        }
        
    # Step 1: Copy script to container
    try:
        subprocess.run(["docker", "cp", str(spark_job_path), "spark-master:/tmp/restore_job.py"], check=True)
    except Exception as e:
        return {"success": False, "error": f"Failed to copy restore script to container: {e}"}
    
    # Step 2: Build HDFS source path (Internal Docker URL)
    hdfs_snapshot_path = f"hdfs://namenode:9000/backups/{repo_name}/{full_hash}/"
    
    # Step 3: Run via Docker Exec
    # We use -e to pass the API key securely
    cmd = [
        "docker", "exec", 
        "-e", f"GITHUB_API_KEY={github_api_key}",
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--conf", "spark.executor.memory=2g",
        "--conf", "spark.driver.memory=1g",
        "/tmp/restore_job.py",
        "--commit-hash", full_hash,
        "--repo-name", repo_name,
        "--hdfs-path", hdfs_snapshot_path,
        "--target-url", target_repo_url,
    ]
    
    print(f"       ⚙️ Docker-Spark Command: {' '.join(cmd[:10])} ... [SECURE]")
    
    try:
        # Submit the job
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            return {
                "success": True,
                "message": f"Restore job completed successfully via Docker-Spark. Repository restored to commit {full_hash[:7]} and pushed to {target_repo_url}",
                "stdout": result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout,
                "command": " ".join(cmd[:10])
            }
        else:
            return {
                "success": False,
                "error": f"Spark container job failed with exit code {result.returncode}",
                "stderr": result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr
            }
            
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "error": "Spark container job timed out after 5 minutes"
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to submit Spark job via Docker: {str(e)}"
        }
