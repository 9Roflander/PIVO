# 🍺 PIVO

**Python Intelligent Version Orchestrator** - An AI-powered Git backup & metadata assistant built on a Dockerized Hadoop ecosystem.

PIVO lets you ask natural language questions about your Git repositories, compare file versions, and restore to any commit - all powered by Google Gemini.

![Python](https://img.shields.io/badge/Python-3.10+-blue)
![Docker](https://img.shields.io/badge/Docker-Required-blue)
![License](https://img.shields.io/badge/License-MIT-green)

---

## 🎯 Features

| Capability | Example | Technology |
|------------|---------|------------|
| **Metadata Query** | "Who changed auth.py last week?" | Gemini + Hive SQL |
| **File Comparison** | "Explain changes between commits" | HDFS + difflib + Gemini |
| **Repository Restore** | "Roll back to commit abc123" | Spark + Git |
| **GitHub Backup** | Import repos into the system | Git + WebHDFS |

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                         USER                                  │
│                          ↓                                    │
│                    [ PIVO Agent ]                            │
│                     (main.py)                                 │
│                          ↓                                    │
│              ┌─────────────────────┐                         │
│              │    Gemini API       │                         │
│              │  (LLM Reasoning)    │                         │
│              └─────────────────────┘                         │
│                          ↓                                    │
│    ┌──────────────┬──────────────┬──────────────┐           │
│    │  query_hive  │  file_diff   │   restore    │           │
│    └──────┬───────┴──────┬───────┴──────┬───────┘           │
│           ↓              ↓              ↓                    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │    HIVE     │  │    HDFS     │  │    SPARK    │          │
│  │  :10000     │  │   :9000     │  │   :7077     │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
│                                                              │
│               [ Docker Compose Infrastructure ]              │
└──────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Git
- [Google Gemini API Key](https://makersuite.google.com/app/apikey)

### 1. Clone & Setup

```bash
git clone https://github.com/YOUR_USERNAME/PIVO.git
cd PIVO

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
# Edit .env and add your GEMINI_API_KEY
```

### 3. Start Infrastructure

```bash
docker-compose up -d

# Wait for containers to be healthy (~1 min)
docker-compose ps
```

### 4. Start Background Services

Open two new terminal windows and run:

**Terminal 2: Metadata Service** (Listens for Ingestion Events)
```bash
python run_metadata_service.py
```

**Terminal 3: Audit Logger** (Captures Logs to `logs/`)
```bash
python run_audit_logger.py
```

### 5. Run PIVO Agent

**Terminal 1:**
```bash
python main.py
```

---

## 📥 Ingesting GitHub Repositories

Before querying, you need to backup repositories into the system:

```bash
# Backup latest commit
python ingest.py --repo https://github.com/octocat/Hello-World

# Backup specific commit
python ingest.py --repo https://github.com/user/repo --commit abc1234

# Backup last 5 commits
python ingest.py --repo https://github.com/user/repo --count 5

# Private repository
python ingest.py --repo https://github.com/user/private --github-token YOUR_TOKEN
```

---

## 💬 Example Queries

Once repositories are ingested, ask PIVO:

```
You: List all commits in the Hello-World repo
PIVO: I found 3 commits in Hello-World...

You: Who made the most changes last month?
PIVO: Based on the metadata, user "octocat" made 15 commits...

You: Compare README.md between the first and latest commit
PIVO: The changes include: Added installation instructions...
```

---

## 🐳 Docker Services

| Service | Port | Purpose |
|---------|------|---------|
| HDFS NameNode | 9000, 9870 | File storage |
| Hive Server | 10000 | SQL queries |
| Kafka | 9092 | Message queue |
| Spark Master | 7077, 8080 | Processing |

**Web UIs:**
- HDFS: http://localhost:9870
- Spark: http://localhost:8080

---

## 📁 Project Structure

```
PIVO/
├── docker-compose.yml      # Hadoop ecosystem
├── requirements.txt        # Python dependencies
├── .env.example           # Environment template
├── main.py                # PIVO agent CLI
├── ingest.py              # GitHub import CLI
├── pivo/
│   ├── agent.py           # Gemini orchestrator
│   ├── config.py          # Configuration
│   ├── tools/
│   │   ├── query_hive.py  # Text-to-SQL
│   │   ├── file_diff.py   # Smart diff
│   │   └── restore.py     # Spark restore
│   └── ingest/
│       ├── github_cloner.py
│       ├── hdfs_uploader.py
│       └── hive_cataloger.py
└── spark_jobs/
    └── restore_job.py     # Spark job script
```

---

## 🔧 Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `GEMINI_API_KEY` | Google Gemini API key | Required |
| `PIVO_MODEL` | Gemini model | gemini-2.0-flash |
| `HDFS_HOST` | HDFS hostname | localhost |
| `HIVE_HOST` | Hive hostname | localhost |

---

## 🛠️ Development

```bash
# Run tests
python -m pytest tests/

# Check code style
python -m flake8 pivo/

# Format code
python -m black pivo/
```

---

## 📜 License

MIT License - feel free to use this for your Big Data course projects!

---

## 🙏 Acknowledgments

Built with:
- [Google Gemini](https://ai.google.dev/) - LLM
- [Apache Hadoop](https://hadoop.apache.org/) - HDFS
- [Apache Hive](https://hive.apache.org/) - SQL
- [Apache Spark](https://spark.apache.org/) - Processing
- [Apache Kafka](https://kafka.apache.org/) - Messaging
