"""
Tool C: Restore - Trigger Spark job to restore repository to a specific commit
"""
import os
import subprocess
from typing import Any
from pathlib import Path

from ..config import Config


# Claude Tool Definition
RESTORE_TOOL = {
    "name": "submit_restore_job",
    "description": "Restore a repository to a specific commit state and push it to a new GitHub location. This triggers a Spark job that reads the snapshot from HDFS and pushes to the target repository.",
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
    
    # Build HDFS source path
    hdfs_snapshot_path = f"hdfs://namenode:9000/backups/{repo_name}/{commit_hash}/"
    
    # Prepare environment with API key (secure - not logged)
    env = os.environ.copy()
    env["GITHUB_API_KEY"] = github_api_key
    env["PIVO_RESTORE_JOB"] = "true"
    
    # Build spark-submit command
    # Note: From inside Docker, we use container names (namenode:9000)
    cmd = [
        "spark-submit",
        "--master", config.spark_master,
        "--deploy-mode", "client",
        "--conf", "spark.executor.memory=2g",
        "--conf", "spark.driver.memory=1g",
        str(spark_job_path),
        "--commit-hash", commit_hash,
        "--repo-name", repo_name,
        "--hdfs-path", hdfs_snapshot_path,
        "--target-url", target_repo_url,
    ]
    
    try:
        # Submit the job
        result = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode == 0:
            return {
                "success": True,
                "message": f"Restore job completed successfully. Repository restored to commit {commit_hash[:7]} and pushed to {target_repo_url}",
                "stdout": result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout
            }
        else:
            return {
                "success": False,
                "error": f"Spark job failed with exit code {result.returncode}",
                "stderr": result.stderr[-2000:] if len(result.stderr) > 2000 else result.stderr
            }
            
    except subprocess.TimeoutExpired:
        return {
            "success": False,
            "error": "Spark job timed out after 5 minutes"
        }
    except FileNotFoundError:
        return {
            "success": False,
            "error": "spark-submit not found. Ensure Spark is installed and in PATH."
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Failed to submit Spark job: {str(e)}"
        }
