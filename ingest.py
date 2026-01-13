#!/usr/bin/env python3
"""
PIVO Ingest - Import GitHub repositories into HDFS and Hive

Usage:
    python ingest.py --repo https://github.com/user/repo [OPTIONS]

Options:
    --repo URL          GitHub repository URL (required)
    --commit HASH       Specific commit to backup (default: HEAD)
    --github-token TOK  GitHub token for private repos
    --count N           Number of recent commits to backup (default: 1)
"""
import argparse
import shutil
import sys
from pathlib import Path

from pivo.config import Config
from pivo.ingest.github_cloner import clone_repo, get_commit_metadata, get_recent_commits
from pivo.ingest.hdfs_uploader import upload_to_hdfs
from pivo.ingest.kafka_service import PivoKafkaProducer
from pivo.ingest.hive_cataloger import check_snapshot_exists


def print_banner():
    """Print ingestion banner."""
    print("""
╔═══════════════════════════════════════════════════════════════╗
║  PIVO Ingest - GitHub → HDFS → Kafka (Producer)               ║
╚═══════════════════════════════════════════════════════════════╝
    """)


def ingest_single_commit(
    repo_url: str,
    commit_hash: str | None,
    github_token: str | None,
    config: Config
) -> bool:
    """
    Ingest a single commit (Produce Event).
    """
    local_path = None
    
    try:
        # Step 1: Clone repository
        print(f"[1/4] Cloning {repo_url}...")
        local_path = clone_repo(repo_url, commit_hash, github_token)
        print(f"      Cloned to {local_path}")
        
        # Step 2: Extract metadata
        print("[2/4] Extracting commit metadata...")
        metadata = get_commit_metadata(local_path, repo_url)
        print(f"      Commit: {metadata.commit_hash[:7]} by {metadata.author}")
        
        # Check if already exists
        if check_snapshot_exists(metadata.commit_hash, config):
            print(f"[SKIP] Commit {metadata.commit_hash[:7]} already backed up")
            return True
        
        # Step 3: Upload to HDFS
        print("[3/4] Uploading to HDFS...")
        hdfs_path = upload_to_hdfs(
            local_path,
            metadata.repo_name,
            metadata.commit_hash,
            config
        )
        
        # Step 4: Produce Kafka Event
        print("[4/4] Sending Event to Kafka...")
        producer = PivoKafkaProducer(config)
        producer.connect()
        producer.produce_commit_event(metadata, hdfs_path)
        
        print(f"\n✅ Successfully backed up & queued {metadata.repo_name}@{metadata.commit_hash[:7]}")
        return True
        
    except Exception as e:
        print(f"\n❌ Ingestion failed: {e}")
        return False
    
    finally:
        if local_path and local_path.exists():
            shutil.rmtree(local_path, ignore_errors=True)


def main():
    """Main entry point."""
    print_banner()
    
    parser = argparse.ArgumentParser(description="Import GitHub repositories into PIVO")
    parser.add_argument("--repo", "-r", required=True, help="GitHub repository URL")
    parser.add_argument("--commit", "-c", default=None, help="Specific commit hash")
    parser.add_argument("--github-token", "-t", default=None, help="GitHub token")
    parser.add_argument("--count", "-n", type=int, default=1, help="Commits to backup")
    
    args = parser.parse_args()
    
    try:
        config = Config.from_env()
    except ValueError as e:
        print(f"[ERROR] {e}")
        sys.exit(1)
    
    # Ingest
    if args.count > 1:
        print(f"[INFO] Backing up {args.count} recent commits...")
        local_path = clone_repo(args.repo, None, args.github_token)
        commits = get_recent_commits(local_path, args.repo, args.count)
        
        producer = PivoKafkaProducer(config)
        producer.connect()
        
        success_count = 0
        for metadata in commits:
            if check_snapshot_exists(metadata.commit_hash, config):
                print(f"[SKIP] {metadata.commit_hash[:7]} already exists")
                continue
            
            hdfs_path = upload_to_hdfs(
                metadata.local_path,
                metadata.repo_name,
                metadata.commit_hash,
                config
            )
            
            producer.produce_commit_event(metadata, hdfs_path)
            success_count += 1
        
        shutil.rmtree(local_path, ignore_errors=True)
        print(f"\n✅ Queued {success_count}/{len(commits)} commits for cataloging")
    else:
        success = ingest_single_commit(args.repo, args.commit, args.github_token, config)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
