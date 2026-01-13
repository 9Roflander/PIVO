#!/usr/bin/env python3
"""
PIVO - Python Intelligent Version Orchestrator

Main entry point for the PIVO agent CLI.
"""
import sys
import threading
import queue
from pivo import PIVOAgent, Config

# Global state for notification synchronization
IS_LLM_BUSY = False
NOTIFICATION_QUEUE = queue.Queue()


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

    print(banner)


def start_notification_thread(agent):
    """Starts a background thread to listen for commit notifications."""
    def listener():
        for event in agent.listen_for_notifications():
            repo = event.get('repo_name', 'Unknown Repo')
            commit = event.get('commit_hash', '???')[:7]
            meta = event.get('metadata', {})
            author = meta.get('author', 'Unknown')
            msg = meta.get('message', 'No message')
            
            
            message_text = (
                f"\n\n[🔔] NEW COMMIT DETECTED in {repo}\n"
                f"    Author: {author}\n"
                f"    Commit: {commit}\n"
                f"    Message: {msg}\n"
                f"{'-' * 40}\n"
            )

            if IS_LLM_BUSY:
                # Queue it if LLM is thinking/printing
                NOTIFICATION_QUEUE.put(message_text)
            else:
                # Print immediately if idle
                print(message_text)
                print("You: ", end="", flush=True)

    t = threading.Thread(target=listener, daemon=True)
    t.start()

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
    
    # Start background listener
    start_notification_thread(agent)
    
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
            
            global IS_LLM_BUSY
            IS_LLM_BUSY = True
            try:
                response = agent.chat(user_input)
                print(f"PIVO: {response}")
            finally:
                IS_LLM_BUSY = False
                
            # Flush pending notifications
            while not NOTIFICATION_QUEUE.empty():
                print(NOTIFICATION_QUEUE.get())
                
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
