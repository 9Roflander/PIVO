#!/usr/bin/env python3
"""
PIVO - Python Intelligent Version Orchestrator

Main entry point for the PIVO agent CLI.
"""
import sys
from pivo import PIVOAgent, Config


def print_banner():
    """Print the PIVO startup banner."""
    banner = """
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║   ██████╗ ██╗██╗   ██╗ ██████╗                               ║
║   ██╔══██╗██║██║   ██║██╔═══██╗                              ║
║   ██████╔╝██║██║   ██║██║   ██║                              ║
║   ██╔═══╝ ██║╚██╗ ██╔╝██║   ██║                              ║
║   ██║     ██║ ╚████╔╝ ╚██████╔╝                              ║
║   ╚═╝     ╚═╝  ╚═══╝   ╚═════╝                               ║
║                                                               ║
║   Python Intelligent Version Orchestrator                     ║
║   Your AI-powered Git backup & metadata assistant             ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def main():
    """Main entry point."""
    print_banner()
    
    try:
        config = Config.from_env()
        print("[PIVO] Configuration loaded successfully")
        print(f"[PIVO] Using model: {config.model}")
        print(f"[PIVO] Hive: {config.hive_host}:{config.hive_port}")
        print(f"[PIVO] HDFS: {config.hdfs_url}")
        print()
    except ValueError as e:
        print(f"[ERROR] Configuration error: {e}")
        print("[TIP] Create a .env file with ANTHROPIC_API_KEY=your_key")
        sys.exit(1)
    
    agent = PIVOAgent(config)
    
    print("Type your questions about repository metadata, file changes, or restore operations.")
    print("Commands: 'quit' or 'exit' to exit, 'reset' to clear conversation history.")
    print("-" * 65)
    print()
    
    while True:
        try:
            user_input = input("You: ").strip()
            
            if not user_input:
                continue
            
            if user_input.lower() in ('quit', 'exit', 'q'):
                print("\n[PIVO] Goodbye!")
                break
            
            if user_input.lower() == 'reset':
                agent.reset()
                print("[PIVO] Conversation history cleared.")
                continue
            
            print()
            response = agent.chat(user_input)
            print(f"PIVO: {response}")
            print()
            
        except KeyboardInterrupt:
            print("\n\n[PIVO] Interrupted. Goodbye!")
            break
        except Exception as e:
            print(f"\n[ERROR] {e}")
            print("[PIVO] An error occurred. Try again or type 'reset' to start fresh.")
            print()


if __name__ == "__main__":
    main()
