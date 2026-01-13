"""
PIVO Metadata Service
Listens to Kafka events for new HDFS backups and updates the Metadata Catalog (SQLite).
"""
import time
from pathlib import Path

from pivo.config import Config
from pivo.ingest.kafka_service import PivoKafkaConsumer
from pivo.ingest.hive_cataloger import create_table, insert_snapshot
from pivo.ingest.github_cloner import CommitMetadata

def process_event(event: dict):
    """Callback to process ingest events."""
    if event.get("event_type") != "COMMIT_INGESTED":
        return

    print(f"  -> Processing metadata for {event.get('commit_hash')}")
    
    # Reconstruct Metadata object from JSON
    raw_meta = event.get("metadata", {})
    metadata = CommitMetadata(
        commit_hash=event.get("commit_hash"),
        repo_name=event.get("repo_name"),
        author=raw_meta.get("author"),
        author_email="", # Not passed in event for simplicity, or add it
        commit_message=raw_meta.get("message"),
        commit_timestamp=raw_meta.get("timestamp"),
        files_changed=raw_meta.get("files_changed", []),
        additions=raw_meta.get("additions", 0),
        deletions=raw_meta.get("deletions", 0),
        branch=raw_meta.get("branch"),
        local_path=None # Not needed for cataloging
    )
    
    hdfs_path = event.get("hdfs_path")
    
    # Insert into Catalog
    config = Config.from_env()
    create_table(config) # Ensure table exists
    insert_snapshot(metadata, hdfs_path, config)
    print("  -> Catalog updated successfully ✅")


def main():
    print("╔═══════════════════════════════════════════════════════════════╗")
    print("║  PIVO Metadata Service (Kafka Consumer)                       ║")
    print("╚═══════════════════════════════════════════════════════════════╝")
    
    config = Config.from_env()
    
    consumer = PivoKafkaConsumer(config)
    
    # Retry loop for initial connection
    while True:
        try:
            consumer.start(callback=process_event)
            break # If start returns, consumer stopped (or failed connect)
        except Exception as e:
            print(f"Service crashed: {e}. Restarting in 5s...")
            time.sleep(5)

if __name__ == "__main__":
    main()
