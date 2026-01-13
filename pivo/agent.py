"""
PIVO Agent - Main orchestrator for LLM-driven tool execution using Gemini
"""
import json
from typing import Any
import google.generativeai as genai

from .config import Config
from .tools import TOOLS, TOOL_FUNCTIONS


SYSTEM_PROMPT = """You are PIVO (Python Intelligent Version Orchestrator), an AI assistant specialized in managing Git repository backups stored in a Hadoop ecosystem.

You have access to three tools:

1. **query_hive**: Query repository metadata using natural language. The data is stored in Hive tables and includes commit information, authors, timestamps, and file changes.

2. **get_file_diff**: Compare two versions of a file stored in HDFS. Provide the HDFS paths and you'll get a plain-English explanation of what changed.

3. **submit_restore_job**: Restore a repository to a specific commit and push it to a new GitHub location. This requires the commit hash, repository name, target URL, and a GitHub API key.

When users ask questions:
- For metadata queries (who, what, when, how many), use query_hive
- For understanding file changes, use get_file_diff
- For restoring/reverting repositories, use submit_restore_job

Always explain what you're doing and interpret results for the user in a helpful way."""


class PIVOAgent:
    """Main PIVO agent that orchestrates LLM and tool execution."""
    
    def __init__(self, config: Config):
        self.config = config
        genai.configure(api_key=config.gemini_api_key)
        
        # Create the model with tools
        self.model = genai.GenerativeModel(
            model_name=config.model,
            system_instruction=SYSTEM_PROMPT,
            tools=TOOLS
        )
        self.chat_session = self.model.start_chat(history=[])
    
    def _execute_tool(self, tool_name: str, tool_input: dict[str, Any]) -> Any:
        """Execute a tool and return its result."""
        if tool_name not in TOOL_FUNCTIONS:
            return {"error": f"Unknown tool: {tool_name}"}
        
        func = TOOL_FUNCTIONS[tool_name]
        
        # Add config to the tool call
        return func(**tool_input, config=self.config)
    
    def chat(self, user_message: str) -> str:
        """
        Process a user message and return the agent's response.
        
        Implements the tool-use loop:
        1. Send message to Gemini with tools
        2. If Gemini wants to use a tool, execute it
        3. Send tool result back to Gemini
        4. Repeat until Gemini provides a final response
        """
        response = self.chat_session.send_message(user_message)
        
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
            response = self.chat_session.send_message(function_responses)
        
        # Extract final text if loop exits normally
        text_parts = [
            part.text 
            for part in response.candidates[0].content.parts 
            if hasattr(part, 'text')
        ]
        return "\n".join(text_parts) if text_parts else "I completed the task but have no additional response."
    
    def reset(self):
        """Clear conversation history."""
        self.chat_session = self.model.start_chat(history=[])
