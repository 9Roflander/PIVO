"""
PIVO Tools - Functions callable by the LLM
"""
import google.generativeai as genai

from .query_hive import query_hive
from .file_diff import get_file_diff
from .restore import submit_restore_job

# Gemini Tool Definitions using function declarations
query_hive_func = genai.protos.FunctionDeclaration(
    name="query_hive",
    description="Query the repository metadata catalog using natural language. Converts questions into HiveQL and executes against the repo_snapshots table. Use this for questions like 'Who changed the payment API yesterday?' or 'List all commits by a specific person.'",
    parameters=genai.protos.Schema(
        type=genai.protos.Type.OBJECT,
        properties={
            "question": genai.protos.Schema(
                type=genai.protos.Type.STRING,
                description="The natural language question about repository metadata"
            )
        },
        required=["question"]
    )
)

file_diff_func = genai.protos.FunctionDeclaration(
    name="get_file_diff",
    description="Compare two versions of a file stored in HDFS and get a plain-English explanation of the changes. Provide the HDFS paths to both file versions.",
    parameters=genai.protos.Schema(
        type=genai.protos.Type.OBJECT,
        properties={
            "file_path_a": genai.protos.Schema(
                type=genai.protos.Type.STRING,
                description="HDFS path to the first (older) version of the file"
            ),
            "file_path_b": genai.protos.Schema(
                type=genai.protos.Type.STRING,
                description="HDFS path to the second (newer) version of the file"
            ),
            "context_lines": genai.protos.Schema(
                type=genai.protos.Type.INTEGER,
                description="Number of context lines around changes (default: 3)"
            )
        },
        required=["file_path_a", "file_path_b"]
    )
)

restore_func = genai.protos.FunctionDeclaration(
    name="submit_restore_job",
    description="Restore a repository to a specific commit state and push it to a new GitHub location. This triggers a Spark job that reads the snapshot from HDFS and pushes to the target repository.",
    parameters=genai.protos.Schema(
        type=genai.protos.Type.OBJECT,
        properties={
            "commit_hash": genai.protos.Schema(
                type=genai.protos.Type.STRING,
                description="The git commit hash to restore to"
            ),
            "repo_name": genai.protos.Schema(
                type=genai.protos.Type.STRING,
                description="Name of the repository to restore"
            ),
            "target_repo_url": genai.protos.Schema(
                type=genai.protos.Type.STRING,
                description="GitHub URL where the restored repo should be pushed"
            ),
            "github_api_key": genai.protos.Schema(
                type=genai.protos.Type.STRING,
                description="GitHub Personal Access Token with repo write permissions"
            )
        },
        required=["commit_hash", "repo_name", "target_repo_url", "github_api_key"]
    )
)

# Tool registry for Gemini
TOOLS = [genai.protos.Tool(function_declarations=[
    query_hive_func,
    file_diff_func,
    restore_func
])]

# Function dispatch map
TOOL_FUNCTIONS = {
    "query_hive": query_hive,
    "get_file_diff": get_file_diff,
    "submit_restore_job": submit_restore_job,
}
