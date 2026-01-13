"""
Tool A: Query Hive - Text-to-SQL for metadata queries using Gemini
"""
from typing import Any
from pyhive import hive
import google.generativeai as genai

from ..config import Config


# Hive table schema for SQL generation
REPO_SNAPSHOTS_SCHEMA = """
Table: repo_snapshots
Columns:
  - commit_hash: STRING (PRIMARY KEY) - Git commit SHA
  - repo_name: STRING - Name of the repository
  - author: STRING - Commit author name
  - author_email: STRING - Commit author email
  - commit_message: STRING - Full commit message
  - commit_timestamp: TIMESTAMP - When the commit was made
  - files_changed: ARRAY<STRING> - List of file paths that were changed
  - additions: INT - Lines added
  - deletions: INT - Lines deleted
  - branch: STRING - Branch name
  - hdfs_path: STRING - Path to snapshot in HDFS

Example queries:
- SELECT * FROM repo_snapshots WHERE author = 'John' ORDER BY commit_timestamp DESC LIMIT 10
- SELECT commit_hash, commit_message FROM repo_snapshots WHERE commit_timestamp >= '2024-01-01'
"""


def query_hive(question: str, config: Config) -> dict[str, Any]:
    """
    Execute a natural language query against Hive.
    
    1. Send question + schema to Gemini for SQL generation
    2. Execute generated SQL against Hive
    3. Return formatted results
    """
    # Step 1: Generate SQL from natural language
    genai.configure(api_key=config.gemini_api_key)
    model = genai.GenerativeModel(config.model)
    
    sql_prompt = f"""You are a SQL expert. Given the following Hive table schema and a natural language question, 
generate a valid HiveQL query to answer the question.

{REPO_SNAPSHOTS_SCHEMA}

Question: {question}

Rules:
- Return ONLY the SQL query, no explanations
- Use HiveQL syntax (compatible with Hive 2.3)
- Limit results to 100 rows maximum unless specified otherwise
- Use proper date/timestamp comparisons

SQL Query:"""

    response = model.generate_content(sql_prompt)
    sql_query = response.text.strip()
    
    # Remove markdown code blocks if present
    if sql_query.startswith("```"):
        lines = sql_query.split("\n")
        sql_query = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
    
    # Step 2: Execute query against Hive
    try:
        conn = hive.connect(
            host=config.hive_host,
            port=config.hive_port,
            username="hive",
            auth="NOSASL"
        )
        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        # Fetch results
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        # Format results
        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))
        
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
