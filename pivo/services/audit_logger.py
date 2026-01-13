"""
Audit Logger Service - PIVO
---------------------------
Consumes events from Kafka (pivo-audit-logs) and writes them to 
structured log files for user transparency.
"""
import json
import sys
from pathlib import Path
from datetime import datetime

try:
    from kafka import KafkaConsumer
except ImportError:
    print("Error: kafka-python is required. pip install kafka-python")
    sys.exit(1)

from pivo.config import Config

# Setup paths
LOG_DIR = Path("logs")
CHAT_LOG = LOG_DIR / "chat_history.log"
TOOL_LOG_DIR = LOG_DIR / "tools"
INGESTION_LOG = LOG_DIR / "ingestion.log"


def setup_directories():
    LOG_DIR.mkdir(exist_ok=True)
    TOOL_LOG_DIR.mkdir(exist_ok=True)

def append_to_log(file_path: Path, content: str):
    """Append a line to a log file with flushing."""
    try:
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(content + "\n")
    except Exception as e:
        print(f"[ERROR] Could not write to {file_path}: {e}")

def process_event(event: dict):
    # Normalize event type (Agent uses 'type', Ingest uses 'event_type')
    event_type = event.get("type") or event.get("event_type") or "UNKNOWN"

    timestamp = event.get("timestamp", datetime.now().isoformat())
    
    # Common format: [TIMESTAMP] [TYPE] Content...
    prefix = f"[{timestamp}] [{event_type}]"
    
    print(f"[AUDIT] Processing {event_type}...")
    
    if event_type == "SESSION_START":
        log_entry = f"{'-'*60}\n{prefix} New Session Started\n{'-'*60}"
        append_to_log(CHAT_LOG, log_entry)

    elif event_type == "CHAT_MESSAGE":
        role = event.get("role", "UNKNOWN")
        content = event.get("content", "").strip()
        log_entry = f"{prefix} {role}: {content}\n"
        append_to_log(CHAT_LOG, log_entry)

    elif event_type == "TOOL_EXECUTION":
        tool = event.get("tool_name", "unknown_tool")
        args = json.dumps(event.get("arguments", {}), ensure_ascii=False)
        result = event.get("result_preview", "")
        command = event.get("command")
        
        # Write to tool-specific log
        tool_log_path = TOOL_LOG_DIR / f"{tool}.log"
        
        log_entry = f"{prefix}\n   Args:   {args}"
        if command:
            log_entry += f"\n   CMD:    {command}"
        log_entry += f"\n   Result: {result}\n"
        
        append_to_log(tool_log_path, log_entry)
        
        # Also summarize in chat log for context
        append_to_log(CHAT_LOG, f"{prefix} Executed tool: {tool}")
        if command:
             append_to_log(CHAT_LOG, f"   ↳ CMD: {command}")

    elif event_type == "COMMIT_INGESTED":
        repo = event.get("repo_name", "UNKNOWN")
        commit = event.get("commit_hash", "UNKNOWN")
        meta = event.get("metadata", {})
        author = meta.get("author", "unknown")
        msg = meta.get("message", "no message")
        
        log_entry = f"{prefix}\n   Repo:   {repo}\n   Commit: {commit}\n   Author: {author}\n   Msg:    {msg}\n"
        append_to_log(INGESTION_LOG, log_entry)
        # Also notify chat log
        append_to_log(CHAT_LOG, f"{prefix} New Commit Ingested: {commit[:7]} by {author}")

    else:
        # Generic fallback
        append_to_log(LOG_DIR / "system.log", f"{prefix} {json.dumps(event)}")

def main():
    setup_directories()
    # Ensure CWD is project root or handle paths carefully
    # Assuming run from root via python -m pivo.services.audit_logger
    
    try:
        config = Config.from_env()
    except Exception as e:
        print(f"[ERROR] Loading config: {e}")
        return

    print(f"[*] Starting Audit Logger...")
    print(f"[*] Connected into Kafka: {config.kafka_bootstrap_servers}")
    print(f"[*] Logs will be written to: {LOG_DIR.absolute()}")


    try:
        consumer = KafkaConsumer(
            'pivo-audit-logs', 'pivo-commit-events',
            bootstrap_servers=config.kafka_bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='pivo-audit-group-v1',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        for message in consumer:
            process_event(message.value)
            
    except KeyboardInterrupt:
        print("\n[!] Stopping Audit Logger")
    except Exception as e:
        print(f"[CRITICAL] Consumer failed: {e}")

if __name__ == "__main__":
    main()
