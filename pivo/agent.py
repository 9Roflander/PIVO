"""
PIVO Agent - Main orchestrator for LLM-driven tool execution using Gemini
"""
import json
import time
import random
from typing import Any
try:
    from kafka import KafkaProducer, KafkaConsumer
except ImportError:
    KafkaProducer = None
    KafkaConsumer = None
from google import genai
from google.genai import types
from google.api_core import exceptions
import datetime


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
        # Initialize Gemini Client
        self.client = genai.Client(api_key=self.config.gemini_api_key)
        
        # Initialize Kafka Producer
        self.producer = None
        if KafkaProducer:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.config.kafka_bootstrap_servers
                )
            except Exception as e:
                print(f"[PIVO] Warning: Kafka not available for logging: {e}")

        # Discover tracked repositories and activity for context
        self.system_context = self._get_rich_context()
        self.start_chat() # Start the chat session with context
    
    def _log_event(self, event_type: str, payload: dict):
        """Log an event to Kafka."""
        if not self.producer:
            return
            
        message = {
            "type": event_type,
            "timestamp": datetime.datetime.now().isoformat(),
            **payload
        }
        
        try:
            self.producer.send('pivo-audit-logs', json.dumps(message).encode('utf-8'))
        except Exception as e:
            print(f"[PIVO] Kafka log error: {e}")

    def listen_for_notifications(self):
        """Yields commit events from Kafka."""
        if not KafkaConsumer: return
            
        try:
            consumer = KafkaConsumer(
                'pivo-commit-events',
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                # Start from now, don't replay old notifications
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            for message in consumer:
                if message.value.get("event_type") == "COMMIT_INGESTED":
                    yield message.value
        except Exception:
            pass


        
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
        history.append(types.Content(
            role="user",
            parts=[types.Part(text=SYSTEM_PROMPT)]
        ))
        history.append(types.Content(
            role="model",
            parts=[types.Part(text="Understood. I am PIVO, an AI assistant specialized in managing Git repository backups.")]
        ))

        if self.system_context:
            history.append(types.Content(
                role="user",
                parts=[types.Part(text=self.system_context)]
            ))
            history.append(types.Content(
                role="model",
                parts=[types.Part(text="Understood. I have updated my context with the tracked repositories and recent activity.")]
            ))
            
        # Disable auto-calling to use our manual loop
        # Note: In new SDK, we pass tools in config
        self.chat_session = self.client.chats.create(
            model=self.config.model,
            config=types.GenerateContentConfig(tools=TOOLS),
            history=history
        )
        
        self._log_event("SESSION_START", {"context": "new_session"})

    
    def _execute_tool(self, tool_name: str, tool_input: dict[str, Any]) -> Any:
        """Execute a tool and return its result."""
        if tool_name not in TOOL_FUNCTIONS:
            return {"error": f"Unknown tool: {tool_name}"}
        
        func = TOOL_FUNCTIONS[tool_name]
        
        # Add config to the tool call
        result = func(**tool_input, config=self.config)
        
        # Extract technical command for logging if present
        technical_command = None
        if isinstance(result, dict):
            technical_command = result.get("command") or result.get("sql_query")
            
        self._log_event("TOOL_EXECUTION", {
            "tool_name": tool_name, 
            "arguments": tool_input,
            "command": technical_command,
            "result_preview": str(result)[:200]
        })
        
        return result

    
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
        self._log_event("CHAT_MESSAGE", {"role": "USER", "content": user_message})
        
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
                if part.function_call and part.function_call.name
            ]
            
            if not function_calls:
                # No more function calls, extract text response
                break
            
            # Execute each function call
            function_responses = []
            for fc in function_calls:
                tool_name = fc.name
                tool_args = dict(fc.args)
                
                print(f"[PIVO] Executing tool: {tool_name}")
                # Arguments hidden, tool will log technical command
                
                result = self._execute_tool(tool_name, tool_args)
                
                function_responses.append(
                    types.Part(
                        function_response=types.FunctionResponse(
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
            if part.text
        ]

        
        # Log final response text requires buffering or capturing return...
        # Since we return directly, let's just log "AGENT_RESPONSE" before returning
        # Note: Ideally we capture the text first.
        
        # Refactoring slightly to allow logging:
        final_text = "\n".join(text_parts) if text_parts else "I completed the task but have no additional response."
        self._log_event("CHAT_MESSAGE", {"role": "AGENT", "content": final_text})
        if self.producer:
            self.producer.flush()
        return final_text

    
    def reset(self):
        """Clear conversation history and restart with context."""
        self.start_chat()
