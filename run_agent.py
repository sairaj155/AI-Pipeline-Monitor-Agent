"""
run_agent.py
─────────────
THE MAIN FILE. Run this.

Usage:
  python run_agent.py --once          # run one cycle and exit
  python run_agent.py                 # run continuously every N minutes
  python run_agent.py --detect-only   # detect but don't repair
"""
import argparse, time, schedule
from rich.console import Console
from src.models import init_db
from src.agent.agent import PipelineAgent
from src.config import config

console = Console()

def main():
    parser = argparse.ArgumentParser(description="AI Pipeline Agent")
    parser.add_argument("--once",        action="store_true", help="Run one cycle and exit")
    parser.add_argument("--detect-only", action="store_true", help="Detect anomalies but skip repair")
    args = parser.parse_args()

    if args.detect_only:
        import os; os.environ["AUTO_REPAIR"] = "false"
        config.AUTO_REPAIR = False

    init_db()
    agent = PipelineAgent()

    if args.once:
        agent.run()
        return

    interval = config.CHECK_INTERVAL_MINUTES
    console.print(f"[bold]Agent started[/bold] — checking every {interval} minutes. Ctrl+C to stop.\n")
    agent.run()
    schedule.every(interval).minutes.do(agent.run)
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    main()
