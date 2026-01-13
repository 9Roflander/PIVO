"""
PIVO Tools - Functions callable by the LLM
"""
from google import genai
from google.genai import types

from .query_hive import query_hive
from .file_diff import get_file_diff
from .restore import submit_restore_job
from .read_file import get_file_content

# Gemini Tool Definitions using function declarations
query_hive_func = types.FunctionDeclaration(
    name="query_hive",
    description="Query the repository metadata catalog using natural language. Converts questions into HiveQL and executes against the repo_snapshots table. Use this for questions like 'Who changed the payment API yesterday?' or 'List all commits by a specific person.'",
    parameters=types.Schema(
        type="OBJECT",
        properties={
            "question": types.Schema(
                type="STRING",
                description="The natural language question about repository metadata"
            )
        },
        required=["question"]
    )
)

file_diff_func = types.FunctionDeclaration(
    name="get_file_diff",
    description="Compare two versions of a file stored in HDFS and get a plain-English explanation of the changes. Provide the HDFS paths to both file versions.",
    parameters=types.Schema(
        type="OBJECT",
        properties={
            "file_path_a": types.Schema(
                type="STRING",
                description="HDFS path to the first (older) version of the file"
            ),
            "file_path_b": types.Schema(
                type="STRING",
                description="HDFS path to the second (newer) version of the file"
            ),
            "context_lines": types.Schema(
                type="INTEGER",
                description="Number of context lines around changes (default: 3)"
            )
        },
        required=["file_path_a", "file_path_b"]
    )
)

read_file_func = types.FunctionDeclaration(
    name="get_file_content",
    description="Read the full content of a specific file from HDFS. Use this to see what is currently in a file or when diff is unavailable. Requires repository name and file path.",
    parameters=types.Schema(
        type="OBJECT",
        properties={
            "file_path": types.Schema(
                type="STRING",
                description="Relative path of the file (e.g. 'docker-compose.yml')"
            ),
            "repo_name": types.Schema(
                type="STRING",
                description="Name of the repository (e.g. 'PIVO')"
            ),
            "commit_hash": types.Schema(
                type="STRING",
                description="Specific commit hash to read from (optional, defaults to latest)"
            )
        },
        required=["file_path", "repo_name"]
    )
)

restore_func = types.FunctionDeclaration(
    name="submit_restore_job",
    description="Restore a repository to a specific commit state and push it to a new GitHub location. This triggers a Spark job that reads the snapshot from HDFS and pushes to the target repository. CRITICAL: The target GitHub repository MUST be completely empty (no branches/tags), otherwise the operation will fail to prevent overwriting existing history.",
    parameters=types.Schema(
        type="OBJECT",
        properties={
            "commit_hash": types.Schema(
                type="STRING",
                description="The git commit hash to restore to"
            ),
            "repo_name": types.Schema(
                type="STRING",
                description="Name of the repository to restore"
            ),
            "target_repo_url": types.Schema(
                type="STRING",
                description="GitHub URL where the restored repo should be pushed"
            ),
            "github_api_key": types.Schema(
                type="STRING",
                description="GitHub Personal Access Token with repo write permissions"
            )
        },
        required=["commit_hash", "repo_name", "target_repo_url", "github_api_key"]
    )
)

# Tool registry for Gemini
TOOLS = [types.Tool(function_declarations=[
    query_hive_func,
    file_diff_func,
    read_file_func,
    restore_func
])]

# Function dispatch map
TOOL_FUNCTIONS = {
    "query_hive": query_hive,
    "get_file_diff": get_file_diff,
    "get_file_content": get_file_content,
    "submit_restore_job": submit_restore_job,
}
