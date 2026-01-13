"""
Tool B: File Diff - Smart comparison of file versions in HDFS using Gemini
"""
import difflib
import subprocess
from typing import Any
from google import genai

from ..config import Config


def read_hdfs_file(path: str) -> str:
    """Read a text file from HDFS using docker exec."""
    try:
        cmd = ["docker", "exec", "namenode", "hdfs", "dfs", "-cat", path]
        print(f"       ⚙️ HDFS Command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        
        if result.returncode != 0:
            raise Exception(f"HDFS Error: {result.stderr}")
            
        return result.stdout
    except subprocess.TimeoutExpired:
        raise Exception("Timeout reading file from HDFS")


def compute_diff(text_a: str, text_b: str, context_lines: int = 3) -> str:
    """Compute unified diff between two text versions."""
    lines_a = text_a.splitlines(keepends=True)
    lines_b = text_b.splitlines(keepends=True)
    
    diff = difflib.unified_diff(
        lines_a, 
        lines_b,
        fromfile='version_a',
        tofile='version_b',
        lineterm='',
        n=context_lines
    )
    
    return ''.join(diff)


def get_file_diff(
    file_path_a: str, 
    file_path_b: str, 
    config: Config,
    context_lines: int = 3
) -> dict[str, Any]:
    """
    Compare two file versions from HDFS and explain differences.
    """
    try:
        # Step 1: Fetch both files from HDFS (via docker exec)
        content_a = read_hdfs_file(file_path_a)
        content_b = read_hdfs_file(file_path_b)
        
        # Step 2: Compute diff locally
        diff_text = compute_diff(content_a, content_b, context_lines)
        
        if not diff_text:
            return {
                "success": True,
                "has_changes": False,
                "summary": "The files are identical - no changes detected."
            }
        
        # Truncate diff if too large (to fit in context window)
        max_diff_chars = 8000
        truncated = False
        if len(diff_text) > max_diff_chars:
            diff_text = diff_text[:max_diff_chars]
            truncated = True
        
        # Step 3: Send diff to Gemini for explanation
        client = genai.Client(api_key=config.gemini_api_key)
        
        explain_prompt = f"""Analyze this code diff and explain the changes in plain English.
Focus on:
1. What functionality was added, removed, or modified
2. Why these changes might have been made (if apparent)
3. Any potential impact on the codebase

Diff:
```diff
{diff_text}
```

{"Note: The diff was truncated due to size. This is a partial view." if truncated else ""}

Provide a concise but informative summary:"""

        response = client.models.generate_content(
            model=config.model,
            contents=explain_prompt
        )
        summary = response.text.strip()
        
        # Count changes
        additions = diff_text.count('\n+') - diff_text.count('\n+++')
        deletions = diff_text.count('\n-') - diff_text.count('\n---')
        
        return {
            "success": True,
            "has_changes": True,
            "additions": additions,
            "deletions": deletions,
            "truncated": truncated,
            "summary": summary,
            "raw_diff": diff_text if len(diff_text) < 2000 else "[Diff too large to include]",
            "command": f"docker exec namenode hdfs dfs -cat {file_path_a} (and {file_path_b})"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }
