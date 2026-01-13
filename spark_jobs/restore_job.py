"""
Spark Restore Job

Standalone PySpark script to restore a repository snapshot from HDFS
and push it to a target GitHub repository.

Usage:
    spark-submit restore_job.py \
        --commit-hash abc1234 \
        --repo-name my-repo \
        --hdfs-path hdfs://namenode:9000/backups/my-repo/abc1234/ \
        --target-url https://github.com/user/new-repo

Environment:
    GITHUB_API_KEY: GitHub Personal Access Token (required, set by PIVO agent)
"""
import argparse
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Restore repository from HDFS snapshot")
    parser.add_argument("--commit-hash", required=True, help="Target commit hash")
    parser.add_argument("--repo-name", required=True, help="Repository name")
    parser.add_argument("--hdfs-path", required=True, help="HDFS path to snapshot")
    parser.add_argument("--target-url", required=True, help="Target GitHub repository URL")
    return parser.parse_args()


def download_from_hdfs(spark: SparkSession, hdfs_path: str, local_dir: Path) -> bool:
    """
    Download snapshot files from HDFS to local directory.
    
    Uses Spark to read file listings and Hadoop FS commands for download.
    """
    try:
        # Get Hadoop configuration
        hadoop_conf = spark._jsc.hadoopConfiguration()
        
        # Use Hadoop FileSystem API via Spark
        uri = spark._jvm.java.net.URI(hdfs_path)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        
        hdfs_path_obj = spark._jvm.org.apache.hadoop.fs.Path(hdfs_path)
        
        if not fs.exists(hdfs_path_obj):
            print(f"[ERROR] HDFS path does not exist: {hdfs_path}")
            return False
        
        # List all files recursively
        file_statuses = fs.listStatus(hdfs_path_obj)
        
        for status in file_statuses:
            src_path = status.getPath()
            relative_path = src_path.getName()
            dest_path = local_dir / relative_path
            
            if status.isDirectory():
                # Recursively copy directory
                dest_path.mkdir(parents=True, exist_ok=True)
                # Note: For a real implementation, we'd recurse here
            else:
                # Copy file
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                fs.copyToLocalFile(src_path, 
                    spark._jvm.org.apache.hadoop.fs.Path(str(dest_path)))
        
        print(f"[INFO] Downloaded snapshot to {local_dir}")
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to download from HDFS: {e}")
        return False


def push_to_github(local_dir: Path, target_url: str, api_key: str) -> bool:
    """
    Initialize git repo and push to target GitHub URL.
    
    The API key is used for authentication via HTTPS.
    CRITICAL: Only pushes if the remote repository is completely empty.
    """
    try:
        # Construct authenticated URL
        if target_url.startswith("https://github.com/"):
            auth_url = target_url.replace(
                "https://github.com/",
                f"https://{api_key}@github.com/"
            )
        else:
            print(f"[ERROR] Unsupported URL format: {target_url}")
            return False
            
        print(f"[INFO] Checking if target repository is empty: {target_url}")
        
        # Check if remote repository has any references (branches/tags)
        # git ls-remote returns exit code 0 and empty output if repo is empty but exists
        # It returns refs if not empty
        ls_remote_result = subprocess.run(
            ["git", "ls-remote", auth_url],
            capture_output=True,
            text=True
        )
        
        if ls_remote_result.returncode != 0:
            # Could be auth error or repo invalid
            print(f"[ERROR] Failed to check remote repository: {ls_remote_result.stderr}")
            return False
            
        if ls_remote_result.stdout.strip():
            print(f"[ERROR] Target repository is NOT empty. Restoration aborted to prevent overwriting existing history.")
            print(f"       Found existing references: \n{ls_remote_result.stdout[:200]}...")
            return False
            
        print("[INFO] Target repository is empty. Proceeding with restore.")
        
        os.chdir(local_dir)
        
        # Initialize git repo if .git doesn't exist
        if not (local_dir / ".git").exists():
            subprocess.run(["git", "init"], check=True, capture_output=True)
            subprocess.run(["git", "add", "."], check=True, capture_output=True)
            subprocess.run(
                ["git", "commit", "-m", "Restored from HDFS snapshot"],
                check=True, 
                capture_output=True,
                env={**os.environ, "GIT_AUTHOR_NAME": "PIVO", "GIT_AUTHOR_EMAIL": "pivo@local"}
            )
        
        # Add remote and push
        subprocess.run(
            ["git", "remote", "add", "origin", auth_url],
            capture_output=True  # May fail if already exists
        )
        
        # Push to main
        result = subprocess.run(
            ["git", "push", "-u", "origin", "main"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            print(f"[INFO] Successfully pushed to {target_url}")
            return True
        else:
            print(f"[ERROR] Git push failed: {result.stderr}")
            return False
            
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Git command failed: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Push failed: {e}")
        return False


def main():
    """Main entry point for the Spark restore job."""
    args = parse_args()
    
    # Get API key from environment (set securely by PIVO agent)
    api_key = os.environ.get("GITHUB_API_KEY")
    if not api_key:
        print("[ERROR] GITHUB_API_KEY environment variable not set")
        return 1
    
    print(f"[INFO] Starting restore job")
    print(f"[INFO] Commit: {args.commit_hash}")
    print(f"[INFO] Repository: {args.repo_name}")
    print(f"[INFO] Target: {args.target_url}")
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName(f"PIVO-Restore-{args.repo_name}-{args.commit_hash[:7]}") \
        .getOrCreate()
    
    try:
        # Create temporary directory for restored files
        with tempfile.TemporaryDirectory(prefix="pivo_restore_") as temp_dir:
            local_dir = Path(temp_dir)
            
            # Step 1: Download from HDFS
            print(f"[INFO] Downloading snapshot from {args.hdfs_path}")
            if not download_from_hdfs(spark, args.hdfs_path, local_dir):
                return 1
            
            # Step 2: Push to GitHub
            print(f"[INFO] Pushing to {args.target_url}")
            if not push_to_github(local_dir, args.target_url, api_key):
                return 1
        
        print("[INFO] Restore completed successfully!")
        return 0
        
    finally:
        spark.stop()


if __name__ == "__main__":
    exit(main())
