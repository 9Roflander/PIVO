# PIVO System Architecture 🏗️

**Last Updated:** 2026-01-13
**Status:** Operational

## 1. High-Level Architecture

PIVO is designed as a hybrid **Intelligent Agent** + **Big Data Ecosystem**. It separates the "Brain" (LLM) from the "Muscle" (Docker Infrastructure).

```mermaid
graph TD
    User[User Terminal] -->|interactive chat| Agent[PIVO Agent (main.py)]
    Agent -->|API Call| Gemini[Google Gemini 2.0 Flash]
    
    subgraph "Ingestion Pipeline"
        IngestCLI[ingest.py] -->|1. Clone| GitHub[GitHub]
        IngestCLI -->|2. Upload Files| HDFS[HDFS NameNode]
        IngestCLI -->|3. Produce Event| Kafka[Kafka Topic]
        Kafka -->|4. Consume Event| Consumer[run_metadata_service.py]
        Consumer -->|5. index Metadata| SQLite[(SQLite DB)]
    end

    subgraph "Agent Tool Execution"
        Agent -->|Determine Tool| Tools
        Tools -->|Query Context| SQLite
        Tools -->|Read/Diff File| HDFS
        Tools -->|Recover| Spark[Spark Cluster]
    end
```

---

## 2. Component Breakdown

### A. The Agent Layer (The Brain) 🧠
*   **`main.py` & `pivo/agent.py`**
    *   **Role:** The interface between the User and the System.
    *   **Function:** Maintains conversation history, handles context, and executes the "Agent Loop" (Thought -> Action -> Observation).
    *   **Communication:**
        *   Sends prompts to **Google Gemini API** (`google.generativeai`).
        *   Calls local Python functions (`pivo.tools.*`).

### B. The Ingestion Layer (The Arms) 💪
This pipeline moves code from GitHub into our storage ecosystem.

1.  **`ingest.py` (Producer)**
    *   **Role:** The entry point for backups.
    *   **Communication:**
        *   **HTTPS** to GitHub (Clone).
        *   **Docker Exec** to `namenode` (Upload to HDFS).
        *   **TCP/9092** to `kafka` (Produce JSON Event).
2.  **`kafka_service.py` (Messaging)**
    *   **Role:** Handles low-level Kafka connectivity.
    *   **Topic:** `pivo-commit-events`.
3.  **`run_metadata_service.py` (Consumer)**
    *   **Role:** Background worker that processes successful uploads.
    *   **Communication:**
        *   **TCP/9092** from `kafka` (Consume Event).
        *   **Local File I/O** to `pivo.db` (Insert Metadata).

### C. The Storage Layer (The Memory) 💾
1.  **HDFS (Hadoop Distributed File System)**
    *   **Role:** Stores the actual raw code files. 
    *   **Structure:** `/backups/{RepoName}/{CommitHash}/{FilePath}`.
    *   **Access Method:** `docker exec namenode hdfs dfs -cat ...`.
2.  **SQLite Metadata Store (`pivo.db`)**
    *   **Role:** An optimized index of all commits, authors, and dates.
    *   **Why SQLite?** Provides sub-millisecond query times and avoids heavy Hive overhead on ARM64 architecture.
    *   **Table:** `repo_snapshots` (repo_name, commit_hash, author, timestamp, hdfs_path, ...).

### D. The Toolset (The Hands) 🛠️
Every time you ask a question, the Agent picks one of these tools:

| Tool Name | Python Function | Capabilities |
|-----------|-----------------|--------------|
| **query_hive** | `pivo.tools.query_hive` | Converts English questions ("Who changed X?") into SQL and runs it against the metadata DB. |
| **get_file_diff** | `pivo.tools.file_diff` | Fetches two files from HDFS and computes a unified diff for the LLM to explain. |
| **get_file_content** | `pivo.tools.read_file` | Reads the full content of a file from HDFS. Used when no diff is available (e.g., new file) or for code analysis. |
| **submit_restore_job** | `pivo.tools.restore` | (Placeholder) Triggers a Spark job to export a commit from HDFS back to a new Git repo. |

---

## 3. Communication Flows

### Scenario 1: "User Ingests a Repo"
1.  **User** runs `python ingest.py ...`.
2.  **Ingest** clones repo to temp folder.
3.  **Ingest** copies files into **HDFS** container via `docker cp` / `hdfs dfs -put`.
4.  **Ingest** connects to **Kafka** container and sends: `{"event": "COMMIT", "hash": "abc", ...}`.
5.  **Metadata Service** (running in background) wakes up, reads the message, and saves details to **SQLite**.

### Scenario 2: "User Asks a Question"
1.  **User** types: *"What changed in the last commit?"*
2.  **Agent** calls `query_hive` tool.
3.  **Tool** queries **SQLite**: `SELECT hash FROM repo_snapshots ORDER BY time DESC LIMIT 2`.
4.  **Agent** calls `get_file_diff` with the two hashes.
5.  **Tool** calls **HDFS**: `hdfs dfs -cat /backups/repo/hash1/file` and `hash2/file`.
6.  **Tool** returns the text diff.
7.  **Gemini** reads the diff and summarizes: *"User Alice added error handling..."*.

## 4. Infrastructure (Docker Compose)
All heavyweight services run in isolated Docker containers:
*   `namenode`, `datanode` (HDFS)
*   `kafka`, `zookeeper` (Messaging)
*   `hive`, `postgres` (Legacy/Compliance)
*   `spark-master`, `spark-worker` (Processing)

The Python Agent runs on the **Host Machine** (Mac) and bridges into this Docker network using `docker exec` and exposed ports.
