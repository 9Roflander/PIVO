"""
Tool A: Query Hive - Text-to-SQL for metadata queries using Anthropic Claude
(Switched to SQLite backend for reliability)
"""
import sqlite3
from typing import Any
import anthropic
from pathlib import Path

from ..config import Config

# Schema for SQL generation
REPO_SNAPSHOTS_SCHEMA = """
Table: repo_snapshots
Columns:
  - commit_hash: TEXT (PRIMARY KEY) - Full 40-character Git commit SHA (use LIKE 'prefix%' for short hashes)
  - repo_name: TEXT - Name of the repository
  - author: TEXT - Commit author name
  - author_email: TEXT - Commit author email
  - commit_message: TEXT - Full commit message
  - commit_timestamp: TEXT - When the commit was made (ISO format)
  - files_changed: TEXT - JSON list of file paths that were changed
  - additions: INTEGER - Lines added
  - deletions: INTEGER - Lines deleted
  - branch: TEXT - Branch name
  - hdfs_path: TEXT - Path to snapshot in HDFS

Example queries:
- SELECT * FROM repo_snapshots WHERE author LIKE '%John%' ORDER BY commit_timestamp DESC LIMIT 10
- SELECT commit_hash, commit_message FROM repo_snapshots WHERE commit_timestamp >= '2024-01-01'
"""

def get_db_path() -> str:
    """Get path to local SQLite database."""
    return str(Path("pivo.db").absolute())


def query_hive(question: str, config: Config) -> dict[str, Any]:
    """
    Execute a natural language query against the metadata store (SQLite).
    """
    # Step 1: Generate SQL from natural language
    client = anthropic.Anthropic(api_key=config.anthropic_api_key)
    
    sql_prompt = f"""You are a SQL expert. Given the following table schema and a natural language question, 
generate a valid SQLite query to answer the question.

{REPO_SNAPSHOTS_SCHEMA}

Question: {question}

Rules:
- Return ONLY the SQL query, no explanations
- Use SQLite syntax
- Limit results to 100 rows maximum unless specified otherwise
- IMPORTANT: Commit hashes are stored as full 40-character SHA strings. If the user provides a SHORT/abbreviated hash (e.g. '2cc2dd9'), you MUST use LIKE 'hash%' instead of = 'hash'. For example: WHERE commit_hash LIKE '2cc2dd9%'

SQL Query:"""

    response = client.messages.create(
        model=config.model,
        max_tokens=1024,
        messages=[{"role": "user", "content": sql_prompt}]
    )
    sql_query = response.content[0].text.strip()
    
    # Remove markdown code blocks if present
    if sql_query.startswith("```"):
        lines = sql_query.split("\n")
        sql_query = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
    
    print(f"       ⚙️ SQL Generate: {sql_query}")
    
    # Step 2: Execute query against SQLite
    try:
        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        # Fetch results
        rows = cursor.fetchall()
        results = [dict(row) for row in rows]
        
        conn.close()
        
        return {
            "success": True,
            "sql_query": sql_query,
            "row_count": len(results),
            "results": results
        }
        
    except Exception as e:
        return {
            "success": False,
            "sql_query": sql_query,
            "error": str(e)
        }
