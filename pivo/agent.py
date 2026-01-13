"""
PIVO Agent - Main orchestrator for LLM-driven tool execution using Gemini
"""
import json
import time
import random
from typing import Any
import google.generativeai as genai
from google.api_core import exceptions

from .config import Config
from .tools import TOOLS, TOOL_FUNCTIONS


SYSTEM_PROMPT = """You are PIVO (Python Intelligent Version Orchestrator), an AI assistant specialized in managing Git repository backups.

You have access to tools for querying metadata (SQLite), checking file diffs (HDFS), reading full file content (HDFS), and restoring repos (Spark).

**RESPONSE FORMATTING RULES:**
1. **Be Visual:** Use Markdown to make answers easy to scan.
2. **Tables:** When listing multiple items (commits, files, repos), ALWAYS use a Markdown table.
3. **Lists:** Use bullet points for file lists or steps.
4. **Highlights:** Bold (**text**) key information like Commit Hashes, Author Names, and Filenames.
5. **Code:** Use backticks for paths, commands, or code snippets.

**Example Response for "Latest Commit":**
| Repository | Date | Author | Commit |
|------------|------|--------|--------|
| **PIVO** | 2024-01-01 | **Alice** | `a1b2c3d` |

**Message:** feat: Added cool feature
**Files Changed:**
* `src/main.py` (+10/-2)
* `README.md` (+5/-0)

**SMART DEFAULTS:**
- **Unspecified Commit:** If the user asks about "changes", "the update", or a file without a commit hash, **ALWAYS assume the LATEST commit**.
- **File Content:** If asked about a file's content or if a diff is unavailable (e.g., new file), use `get_file_content`.

**Action Sequence:**
1. Call `query_hive` to find the latest commit hash and HDFS path.
2. Proceed with the user's request (diff, read, or restore).

Always explain what you're doing and interpret results for the user in this structured way."""


class PIVOAgent:
    """Main PIVO agent that orchestrates LLM and tool execution."""
    
    def __init__(self, config: Config):
        self.config = config
        # Initialize Gemini
        genai.configure(api_key=self.config.gemini_api_key)
        self.model = genai.GenerativeModel(
            model_name=self.config.model,
            tools=TOOLS # Using global TOOLS as per original code
        )
        
        # Discover tracked repositories and activity for context
        self.system_context = self._get_rich_context()
        self.start_chat() # Start the chat session with context
        
    def _get_rich_context(self) -> str:
        """
        Build a rich context string about tracked repos and their recent activity.
        1. List repos from HDFS (source of truth for files)
        2. Query Hive for latest 3 commits per repo (source of truth for metadata)
        """
        context_parts = []
        
        # 1. HDFS Discovery
        hdfs_repos = []
        try:
            import subprocess
            cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", "/backups"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                for line in result.stdout.split('\n'):
                    parts = line.split()
                    if len(parts) >= 8:
                        repo_name = parts[-1].split('/')[-1]
                        hdfs_repos.append(repo_name)
        except Exception:
            pass

        if not hdfs_repos:
            return ""

        context_parts.append(f"You are currently tracking the following repositories in HDFS: {', '.join(hdfs_repos)}.")
        
        # 2. Activity Discovery (Latest 3 commits from SQLite)
        try:
            import sqlite3
            from pathlib import Path
            db_path = Path("pivo.db").absolute()
            
            if db_path.exists():
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("SELECT repo_name, commit_message, author, commit_hash FROM repo_snapshots ORDER BY commit_timestamp DESC LIMIT 3")
                
                rows = cursor.fetchall()
                if rows:
                    activity = "\nLatest global activity across repositories:\n"
                    for row in rows:
                        repo = row['repo_name']
                        msg = row['commit_message'][:50] + "..." if len(row['commit_message']) > 50 else row['commit_message']
                        author = row['author']
                        activity += f"- [{repo}] {msg} (by {author})\n"
                    context_parts.append(activity)
                conn.close()
        except Exception as e:
            print(f"[PIVO] Warning: Could not fetch recent activity: {e}")

        final_context = "System Context: " + " ".join(context_parts)
        
        # TRANSPARENCY: Print what we are feeding the LLM
        print(f"\n[PIVO] 🧠 LLM Context Loaded:\n{'-'*40}\n{final_context}\n{'-'*40}\n")
        
        return final_context

    def start_chat(self):
        """Start a new chat session."""
        history = []
        # Inject system prompt
        history.append({
            "role": "user",
            "parts": [SYSTEM_PROMPT]
        })
        history.append({
            "role": "model",
            "parts": ["Understood. I am PIVO, an AI assistant specialized in managing Git repository backups."]
        })

        if self.system_context:
            history.append({
                "role": "user",
                "parts": [self.system_context]
            })
            history.append({
                "role": "model",
                "parts": ["Understood. I have updated my context with the tracked repositories and recent activity."]
            })
            
        # Disable auto-calling to use our manual loop
        self.chat_session = self.model.start_chat(history=history)
    
    def _execute_tool(self, tool_name: str, tool_input: dict[str, Any]) -> Any:
        """Execute a tool and return its result."""
        if tool_name not in TOOL_FUNCTIONS:
            return {"error": f"Unknown tool: {tool_name}"}
        
        func = TOOL_FUNCTIONS[tool_name]
        
        # Add config to the tool call
        return func(**tool_input, config=self.config)
    
    def _send_message_with_retry(self, message, max_retries=3):
        """Send message with exponential backoff for rate limits."""
        for attempt in range(max_retries):
            try:
                return self.chat_session.send_message(message)
            except exceptions.ResourceExhausted:
                if attempt == max_retries - 1:
                    raise
                wait_time = (2 ** attempt) + random.random()
                print(f"[PIVO] Rate limit hit. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
            except Exception:
                raise

    def chat(self, user_message: str) -> str:
        """
        Process a user message and return the agent's response.
        """
        try:
            response = self._send_message_with_retry(user_message)
        except exceptions.ResourceExhausted:
            return "[ERROR] API Quota exceeded. Please try again in a minute."
            
        # Process tool calls in a loop
        while response.candidates[0].content.parts:
            # Check if there are function calls
            function_calls = [
                part.function_call 
                for part in response.candidates[0].content.parts 
                if hasattr(part, 'function_call') and part.function_call.name
            ]
            
            if not function_calls:
                # No more function calls, extract text response
                text_parts = [
                    part.text 
                    for part in response.candidates[0].content.parts 
                    if hasattr(part, 'text')
                ]
                return "\n".join(text_parts)
            
            # Execute each function call
            function_responses = []
            for fc in function_calls:
                tool_name = fc.name
                tool_args = dict(fc.args)
                
                print(f"[PIVO] Executing tool: {tool_name}")
                # Arguments hidden, tool will log technical command
                
                result = self._execute_tool(tool_name, tool_args)
                
                function_responses.append(
                    genai.protos.Part(
                        function_response=genai.protos.FunctionResponse(
                            name=tool_name,
                            response={"result": json.dumps(result, default=str)}
                        )
                    )
                )
            
            # Send function results back to continue the conversation
            try:
                response = self._send_message_with_retry(function_responses)
            except exceptions.ResourceExhausted:
                return "[ERROR] API Quota exceeded during tool execution."
        
        # Extract final text if loop exits normally
        text_parts = [
            part.text 
            for part in response.candidates[0].content.parts 
            if hasattr(part, 'text')
        ]
        return "\n".join(text_parts) if text_parts else "I completed the task but have no additional response."
    
    def reset(self):
        """Clear conversation history and restart with context."""
        self.start_chat()
