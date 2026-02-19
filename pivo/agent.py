"""
PIVO Agent - Main orchestrator for LLM-driven tool execution using Anthropic Claude
"""
import json
import time
import random
import datetime
from typing import Any

try:
    from kafka import KafkaProducer, KafkaConsumer
except ImportError:  # Optional dependency
    KafkaProducer = None
    KafkaConsumer = None

import anthropic

from .config import Config
from .tools import ANTHROPIC_TOOLS, TOOL_FUNCTIONS


SYSTEM_PROMPT = """You are PIVO (Python Intelligent Version Orchestrator), an AI assistant specialized in managing Git repository backups.

You have access to tools for querying metadata (SQLite), checking file diffs (HDFS), reading full file content (HDFS), restoring repos (Spark), and **backing up new repositories (Ingestion)**.

**RESPONSE FORMATTING RULES:**
1. **Be Visual:** Use Markdown to make answers easy to scan.
2. **Tables:** When listing multiple items, use compact Markdown tables.
3. **Commit Hashes:** ALWAYS truncate to 7 characters (e.g., `a1b2c3d` not the full 40 chars).
4. **Messages:** Keep commit messages under 40 characters. Truncate with "..." if needed.
5. **Lists:** Use bullet points for file lists or steps.
6. **Highlights:** Bold (**text**) key info like Author Names.
7. **Code:** Use backticks for paths, commands, or code snippets.

**Example Table for Commits (FOLLOW THIS FORMAT):**
| Repo | Date | Author | Commit | Message |
|------|------|--------|--------|---------|
| PIVO | 2024-01-01 | **Alice** | `a1b2c3d` | feat: Added cool feature |
| PIVO | 2024-01-02 | **Bob** | `e4f5g6h` | fix: Resolved bug in API |

**SMART DEFAULTS:**
- **Unspecified Commit:** If the user asks about "changes", "the update", or a file without a commit hash, **ALWAYS assume the LATEST commit**.
- **File Content:** If asked about a file's content or if a diff is unavailable (e.g., new file), use `get_file_content`.
- **New Repo:** If a user provides a GitHub URL that isn't in your context, use `ingest_repository` to start tracking it.

**CRITICAL RULES:**
- **NEVER fabricate or hallucinate data.** You MUST call `query_hive` before answering ANY question about commits, files, authors, dates, or repository history. Do NOT guess or make up commit hashes, authors, dates, or messages — even if you have system context loaded. The system context is only a preview; always verify with a tool call.
- If a tool returns an error or empty results, tell the user honestly.

**Action Sequence:**
1. For metadata/files: Call `query_hive` to find context, then proceed with diff, read, or restore.
2. For new backups: Call `ingest_repository` and explain that it is being processed.

Always explain what you're doing and interpret results for the user in this structured way."""


class PIVOAgent:
    """Main PIVO agent that orchestrates LLM and tool execution."""

    def __init__(self, config: Config):
        self.config = config
        # Anthropic client
        self.client = anthropic.Anthropic(api_key=self.config.anthropic_api_key)

        # Kafka producer (optional)
        self.producer = None
        if KafkaProducer:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.config.kafka_bootstrap_servers
                )
            except Exception as e:  # pragma: no cover
                print(f"[PIVO] Warning: Kafka not available for logging: {e}")

        # Context for LLM
        self.system_context = self._get_rich_context()
        self.conversation_history: list[dict[str, Any]] = []

        self._log_event("SESSION_START", {"context": "new_session"})

    def _log_event(self, event_type: str, payload: dict):
        """Log an event to Kafka if available."""
        if not self.producer:
            return

        message = {
            "type": event_type,
            "timestamp": datetime.datetime.now().isoformat(),
            **payload,
        }

        try:
            self.producer.send("pivo-audit-logs", json.dumps(message).encode("utf-8"))
        except Exception as e:  # pragma: no cover
            print(f"[PIVO] Kafka log error: {e}")

    def listen_for_notifications(self):
        """Yield commit events from Kafka (best-effort)."""
        if not KafkaConsumer:
            return

        try:
            consumer = KafkaConsumer(
                "pivo-commit-events",
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                auto_offset_reset="latest",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            for message in consumer:
                if message.value.get("event_type") == "COMMIT_INGESTED":
                    yield message.value
        except Exception:
            return

    def _get_rich_context(self, verbose: bool = True) -> str:
        """Build context from HDFS and SQLite."""
        context_parts: list[str] = []

        # HDFS discovery
        hdfs_repos: list[str] = []
        try:
            import subprocess

            cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-ls", "/backups"]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                for line in result.stdout.split("\n"):
                    parts = line.split()
                    if len(parts) >= 8:
                        repo_name = parts[-1].split("/")[-1]
                        hdfs_repos.append(repo_name)
        except Exception:
            pass

        if not hdfs_repos:
            return ""

        context_parts.append(
            f"You are currently tracking the following repositories in HDFS: {', '.join(hdfs_repos)}."
        )

        # Activity discovery from SQLite
        try:
            import sqlite3
            from pathlib import Path

            db_path = Path("pivo.db").absolute()

            if db_path.exists():
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT repo_name, commit_message, author, commit_hash FROM repo_snapshots "
                    "ORDER BY commit_timestamp DESC LIMIT 10"
                )

                rows = cursor.fetchall()
                if rows:
                    activity = "\nLatest activity (preview only — always use query_hive for accurate data):\n"
                    for row in rows:
                        repo = row["repo_name"]
                        msg = row["commit_message"]
                        if len(msg) > 50:
                            msg = msg[:50] + "..."
                        author = row["author"]
                        short_hash = row["commit_hash"][:7]
                        activity += f"- [{repo}] `{short_hash}` {msg} (by {author})\n"
                    context_parts.append(activity)
                conn.close()
        except Exception as e:  # pragma: no cover
            print(f"[PIVO] Warning: Could not fetch recent activity: {e}")

        final_context = "System Context: " + " ".join(context_parts)

        if verbose:
            print(f"\n[PIVO] 🧠 LLM Context Loaded:\n{'-'*40}\n{final_context}\n{'-'*40}\n")

        return final_context

    def _execute_tool(self, tool_name: str, tool_input: dict[str, Any]) -> Any:
        """Execute a tool and return its result."""
        if tool_name not in TOOL_FUNCTIONS:
            return {"error": f"Unknown tool: {tool_name}"}

        func = TOOL_FUNCTIONS[tool_name]
        result = func(**tool_input, config=self.config)

        technical_command = None
        if isinstance(result, dict):
            technical_command = result.get("command") or result.get("sql_query")

        self._log_event(
            "TOOL_EXECUTION",
            {
                "tool_name": tool_name,
                "arguments": tool_input,
                "command": technical_command,
                "result_preview": str(result)[:200],
            },
        )

        return result

    def _build_system_prompt(self) -> str:
        parts = [SYSTEM_PROMPT]
        if self.system_context:
            parts.append(self.system_context)
        return "\n\n".join(parts)

    def _call_api_with_retry(self, messages: list[dict[str, Any]], max_retries: int = 3):
        """Call Anthropic API with retries on rate limit."""
        for attempt in range(max_retries):
            try:
                return self.client.messages.create(
                    model=self.config.model,
                    max_tokens=4096,
                    system=self._build_system_prompt(),
                    tools=ANTHROPIC_TOOLS,
                    messages=messages,
                )
            except anthropic.RateLimitError:
                if attempt == max_retries - 1:
                    raise
                wait_time = (2**attempt) + random.random()
                print(f"[PIVO] Rate limit hit. Retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
            except Exception:
                raise

    def chat(self, user_message: str) -> str:
        """Process a user message and return the agent's response."""
        # Refresh system context silently (already printed at startup)
        self.system_context = self._get_rich_context(verbose=False)

        self._log_event("CHAT_MESSAGE", {"role": "USER", "content": user_message})

        self.conversation_history.append({"role": "user", "content": user_message})

        try:
            response = self._call_api_with_retry(self.conversation_history)
        except anthropic.RateLimitError:
            self.conversation_history.pop()
            return "[ERROR] API Quota exceeded. Please try again in a minute."

        # Tool-use loop
        while response.stop_reason == "tool_use":
            tool_blocks = [block for block in response.content if block.type == "tool_use"]

            # Save assistant content with tool calls
            self.conversation_history.append({"role": "assistant", "content": response.content})

            tool_results = []
            for block in tool_blocks:
                tool_name = block.name
                tool_input = block.input

                print(f"[PIVO] Executing tool: {tool_name}")

                result = self._execute_tool(tool_name, tool_input)

                tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": json.dumps(result, default=str),
                    }
                )

            # Send tool results back
            self.conversation_history.append({"role": "user", "content": tool_results})

            try:
                response = self._call_api_with_retry(self.conversation_history)
            except anthropic.RateLimitError:
                return "[ERROR] API Quota exceeded during tool execution."

        text_parts = [block.text for block in response.content if hasattr(block, "text")]
        final_text = "\n".join(text_parts) if text_parts else "I completed the task but have no additional response."

        self.conversation_history.append({"role": "assistant", "content": response.content})

        self._log_event("CHAT_MESSAGE", {"role": "AGENT", "content": final_text})
        if self.producer:
            self.producer.flush()
        return final_text

    def reset(self):
        """Clear conversation history."""
        self.conversation_history = []
        self._log_event("SESSION_START", {"context": "reset"})
