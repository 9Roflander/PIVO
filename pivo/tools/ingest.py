"""
Tool D: Ingest - Trigger repository ingestion from the agent
"""
from typing import Any
from ..ingest.cli import ingest_single_commit
from ..config import Config

def ingest_repository(
    repo_url: str,
    commit_hash: str = None,
    github_token: str = None,
    config: Config = None
) -> dict[str, Any]:
    """
    Backup a GitHub repository into the PIVO system.
    This clones the repo, extracts metadata, saves raw files, and creates a full Git bundle for DR.
    """
    if not repo_url.startswith(("https://github.com/", "git@github.com:")):
        return {
            "success": False,
            "error": "Invalid GitHub URL. Must start with https://github.com/"
        }
    
    print(f"       🚀 Agent-Triggered Ingestion: {repo_url}")
    
    try:
        success = ingest_single_commit(
            repo_url=repo_url,
            commit_hash=commit_hash,
            github_token=github_token,
            config=config
        )
        
        if success:
            return {
                "success": True,
                "message": f"Successfully backed up {repo_url}. It is now available for querying and restoration."
            }
        else:
            return {
                "success": False,
                "error": "Ingestion failed. Check service logs for details."
            }
            
    except Exception as e:
        return {
            "success": False,
            "error": f"Tool error during ingestion: {str(e)}"
        }
