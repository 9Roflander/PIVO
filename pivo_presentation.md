# PIVO: Python Intelligent Versioning Orchestrator
## Presentation Script & Outline

### Slide 1: Title & Introduction

**Visuals:**
*   **Title:** PIVO: Python Intelligent Versioning Orchestrator
*   **Subtitle:** Bridging Natural Language with Big Data Infrastructure
*   **Graphics:** A split screen showing a sleek MacBook M4 terminal on the left and a complex Docker/Hadoop cluster diagram on the right, connected by a glowing "Bridge" (The Agent).

**Script:**
"Good morning. I’m here to present **PIVO**—the Python Intelligent Versioning Orchestrator.

We often think of Big Data and AI as separate worlds. Big Data lives in massive, distributed clusters. AI lives in our Python scripts and notebooks. PIVO is the bridge. It connects a local, high-performance MacBook M4 environment to a robust, Dockerized Hadoop ecosystem.

PIVO is not just a backup tool. It is an intelligent operator that understands your intent in natural language and orchestrates enterprise-grade infrastructure—HDFS, Kafka, and Spark—to manage the lifecycle of your code. It turns 'Restore my repo' from a complex engineering ticket into a single sentence."

---

### Slide 2: System Requirements & Infrastructure

**Visuals:**
*   **Header:** Infrastructure Stack
*   **Diagram:** A layered stack component diagram.
    *   **Layer 1 (Compute/Storage):** Docker Containers
        *   `namenode` & `datanode` (HDFS)
        *   `spark-master` & `spark-worker` (Compute)
    *   **Layer 2 (Messaging):** `cp-kafka:7.4.0` & `zookeeper`
    *   **Layer 3 (Metadata):** SQLite (Local M4 Optimization)
    *   **Layer 4 (The Brain):** Python 3.x Client (`pyspark`, `kafka-python`, `google-genai`)

**Script:**
"PIVO isn't a toy script; it runs on top of a fully Dockerized Hadoop Cluster.

At the storage layer, we use **HDFS** with NameNode and DataNode containers to store immutable snapshots of our repositories.
For ingestion, we rely on **Apache Kafka** (specifically `cp-kafka:7.4.0`) to handle high-throughput commit events.
For heavy compute tasks like restoration, we deploy **Apache Spark** with a Master-Worker architecture.

Critically, we have optimized the metadata layer. While traditional Hadoop uses Hive, we’ve replaced the Hive Metastore with a local **SQLite** index. This leverages the M4's fast NVMe storage for millisecond-latency queries while keeping the bulk data distributed."

---

### Slide 3: High-Level Architecture (The "Brain" & "Muscle")

**Visuals:**
*   **Concept:** "Brain vs. Muscle"
*   **Left Side (The Brain):** `agent.py` + LLM (Gemini)
*   **Right Side (The Muscle):** Docker Cluster (HDFS, Spark, Kafka)
*   **Arrow:** `User Request` → `Agent` → `Tool Execution` → `Infrastructure`

**Script:**
"The core architectural philosophy of PIVO is the separation of 'Brain' and 'Muscle'.

The **Brain** is the local `agent.py` script. It holds no data. It has no strength. Its job is purely cognitive: it parses user intent using a Large Language Model and decides *what* needs to be done.

The **Muscle** is the containerized infrastructure. This is where the heavy lifting happens—moving bytes, computing diffs, and processing streams. The Agent never touches the data directly; it commands the Muscle to do it. This decoupling allows us to scale the backend without changing the logic of the frontend agent."

---

### Slide 4: The Orchestrator (Agent Logic)

**Visuals:**
*   **Header:** The ReAct Loop (Reason + Act)
*   **Flowchart:**
    1.  `User Input` ("What changed in main?")
    2.  `LLM Decision` (Selects `query_metadata` or `get_file_diff`)
    3.  `Tool Router` (`_execute_tool` in `agent.py`)
    4.  `Execution` (Subprocess / SQL / API Call)
    5.  `Response` (Formatted Markdown)

**Script:**
"Central to PIVO is `agent.py`. This isn't just a chatbot; it implements the 'Tool Use' pattern.

When you ask PIVO a question, it doesn't hallucinate an answer. It looks at its toolkit—defined in `tools/__init__.py`—and selects the right tool for the job.

The cycle is: Receive Input -> LLM Decision -> Tool Router -> Execution. 
For example, if the Agent decides it needs to read a file, it doesn't just `open()` it. It constructs a command for the `read_file` tool, which executes a Docker command to read from HDFS. The Agent is the commander; the tools are the soldiers."

---

### Slide 5: Tool Deep Dive - Ingestion & Metadata

**Visuals:**
*   **Split Screen:**
    *   **Left (Ingest):** `ingest.py` (CLI) → `pivo-commit-events` (Kafka Topic)
    *   **Right (Metadata):** `agent.py` → `query_metadata` → `pivo.db` (SQLite)

**Script:**
"Let's dive into the tools, starting with how data enters the system.
Our ingestion engine, triggered via `ingest.py`, creates a snapshot of the repository. It pushes these snapshots to HDFS and emits an event to the **Kafka Topic** `pivo-commit-events`. We partition this topic to allow for parallel consumption in the future.

On the read side, we have `query_metadata` (implemented locally as `query_hive`). This tool bridges the gap between natural language and structured data. It takes a question like 'Who committed last?' and uses the LLM to generate a valid **SQL query**, which is then executed against our local SQLite index. This gives us instant answers about codebase history without scanning terabytes of files."

---

### Slide 6: Tool Deep Dive - Storage & Diff

**Visuals:**
*   **Header:** `get_file_diff`
*   **Process:**
    1.  `hdfs dfs -cat /path/to/ver_A`
    2.  `hdfs dfs -cat /path/to/ver_B`
    3.  Python `difflib` (Local Compute)
    4.  LLM Explanation

**Script:**
"Storing files is easy; understanding changes is hard.
Our storage layer uses HDFS, where files are split into immutable blocks.

When a user asks, 'What changed in `main.py`?', PIVO uses the `get_file_diff` tool. It fetches two specific immutable blobs from HDFS using `hdfs dfs -cat`.

Crucially, we compute the diff *locally* using Python's `difflib` before sending it to the LLM. We don't ask the LLM to 'find the difference' between two huge files—that wastes tokens and accuracy. We compute the mathematical diff first, and *then* ask the LLM to explain the semantic meaning of those changes."

---

### Slide 7: Tool Deep Dive - Recovery (Spark)

**Visuals:**
*   **Header:** `restore_repo` (Spark Job)
*   **Diagram:** 
    *   Driver Program (`restore_job.py`)
    *   `SparkContext` distributed reading from HDFS
    *   Push to Target GitHub Repo

**Script:**
"Finally, the most powerful tool: Recovery.
When disaster strikes, we call `restore_repo`. This triggers a dedicated **Apache Spark** job defined in `restore_job.py`.

Why Spark? Because recovery is a batch process that might involve gigabytes of data. Spark's Driver/Executor model allows us to read from HDFS in parallel. The job downloads the snapshot, reconstructs the git history, and pushes it to a fresh GitHub repository.

We treat this as a 'DAG' (Directed Acyclic Graph) of operations: Validation -> Download -> Reconstruction -> Git Push. If any node fails, the job fails gracefully."

---

### Slide 8: Capabilities & Constraints

**Visuals:**
*   **Columns:** 
    *   **Capabilities (Green Check):** Natural Language Querying, Point-in-Time Recovery, Fault-tolerant Ingestion.
    *   **Constraints (Red X):** NO Rewriting History (WORM), NO Direct DB Writes (Agent is Read-Only).

**Script:**
"PIVO is powerful, but it is disciplined.
**Capabilities:** It allows you to query your codebase history as if you were talking to a colleague. It provides point-in-time recovery to any commit hash, guaranteed by the immutability of HDFS.

**Constraints:** We enforce a strict **WORM (Write-Once-Read-Many)** architecture. Once a commit is ingested into HDFS, it is carved in stone. We do NOT allow rewriting history.
Furthermore, the Agent is strictly **Read-Only** regarding metadata. Only the background `kafka_consumer` service is allowed to write to the index, ensuring our catalog never drifts from the actual data in HDFS."

---

### Slide 9: Operational Logs (Simulation)

**Visuals:**
*   **Terminal Window:** specific, color-coded log output showing the sequence of a restore operation.

**Content:**

```text
[2026-01-13 17:25:01] [PIVO-ORCHESTRATOR] [INFO] Received user command: "Restore the 'PIVO' repo to commit a1b2c3d"
[2026-01-13 17:25:02] [PIVO-ORCHESTRATOR] [INFO] Decision: execute_tool(query_metadata, {"question": "Get details for commit a1b2c3d in PIVO repo"})
[2026-01-13 17:25:03] [METADATA-SVC]      [SQL]  SELECT * FROM repo_snapshots WHERE commit_hash = 'a1b2c3d' AND repo_name = 'PIVO'
[2026-01-13 17:25:03] [METADATA-SVC]      [RES]  Found: {files: 12, size: 4MB, hdfs_path: '/backups/PIVO/a1b2c3d/'}
[2026-01-13 17:25:05] [PIVO-ORCHESTRATOR] [INFO] Decision: execute_tool(submit_restore_job, {"commit_hash": "a1b2c3d", "repo_name": "PIVO", "target": "..."})
[2026-01-13 17:25:06] [SPARK-SUBMIT]      [CMD]  spark-submit --master spark://spark-master:7077 --deploy-mode client /app/spark_jobs/restore_job.py --commit-hash a1b2c3d ...
[2026-01-13 17:25:10] [SPARK-DRIVER]      [INFO] AppName: PIVO-Restore-PIVO-a1b2c3d
[2026-01-13 17:25:12] [SPARK-EXECUTOR]    [INFO] Reading from hdfs://namenode:9000/backups/PIVO/a1b2c3d/
[2026-01-13 17:25:25] [SPARK-DRIVER]      [INFO] Push successful to https://github.com/user/restored-pivo
[2026-01-13 17:25:26] [PIVO-ORCHESTRATOR] [LOG]  Sending audit log to Kafka topic 'pivo-audit-logs'
```

**Script:**
"This slide simulates a live restore operation.
First, the **Orchestrator** receives the intent.
Second, it queries the **Metadata Service** to locate the commit in HDFS.
Third, it dispatches the **Spark Job**.
Finally, you see the **Spark Driver** executing the logic and confirming the push. Every step is logged, audited, and recoverable."
