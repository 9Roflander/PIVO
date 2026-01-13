# 🕵️‍♂️ PIVO Troubleshooting & Log Guide

This guide helps you verify if PIVO is tracking your repositories correctly by checking the logs of its various components.

## 1. 👁️ GitHub Watcher (The "Eyes")
*Checks for new commits on GitHub.*

**How to check:**
Since this runs as a background process, check the running process output or log file (if redirected).

```bash
# Check if it's running
ps aux | grep pivo.services.github_watcher

# Run manually (NEW COMMAND)
python -m pivo.services.github_watcher

# If you redirected output to a file (recommended):
tail -f logs/watcher.log
```

**What to look for:**
- `[WATCHER] Scanning branches for...` (Heartbeat)
- `[WATCHER] 🆕 New commit detected...` (Action)
- `[WATCHER] 🚀 Triggering ingestion...` (Hand-off)

---

## 2. 🗄️ HDFS & Ingestion (The "Storage")
*Verifies files are actually saved to the distributed file system.*

**Check HDFS contents:**
```bash
# List all backed-up repositories
docker exec namenode hdfs dfs -ls /backups/

# drill down into a specific repo and commit
docker exec namenode hdfs dfs -ls /backups/YOUR_REPO_NAME/COMMIT_HASH
```

**Run Audit Logger (NEW COMMAND):**
```bash
python -m pivo.services.audit_logger
```

**Run Metadata Service (NEW COMMAND):**
```bash
python -m pivo.services.metadata_service
```

---

## 3. 📝 Metadata Database (The "Brain")
*Verifies the system "knows" about the commit (Author, Message, Branch).*

**Query the local database:**
```bash
# See the last 5 ingested commits
sqlite3 pivo.db "SELECT commit_hash, branch, commit_message FROM repo_snapshots ORDER BY commit_timestamp DESC LIMIT 5;"

# Check for a specific commit hash
sqlite3 pivo.db "SELECT * FROM repo_snapshots WHERE commit_hash LIKE 'abc1234%';"
```

---

## 4. 🧠 PIVO Agent Logs
*See what the AI is thinking and doing.*

The agent logs its tool executions and reasoning to `logs/`.

```bash
# Run the Agent (NEW COMMAND)
python -m pivo.cli

# Tail the tool execution logs
tail -f logs/tools/query_hive.log
```
