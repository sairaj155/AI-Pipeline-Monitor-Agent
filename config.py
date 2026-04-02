"""
config.py — loads all settings from .env
"""
import os
from dotenv import load_dotenv
load_dotenv()

class Config:
    OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY", "")
    ANTHROPIC_API_KEY    = os.getenv("ANTHROPIC_API_KEY", "")
    LLM_PROVIDER         = os.getenv("LLM_PROVIDER", "openai")

    MONITOR_DB_URL       = os.getenv("MONITOR_DB_URL", "sqlite:///./data/pipeline.db")
    AGENT_DB_URL         = os.getenv("AGENT_DB_URL",  "sqlite:///./data/agent.db")

    SLACK_WEBHOOK_URL    = os.getenv("SLACK_WEBHOOK_URL", "")
    SLACK_ENABLED        = os.getenv("SLACK_ENABLED", "false").lower() == "true"

    TABLES_TO_MONITOR    = [t.strip() for t in os.getenv("TABLES_TO_MONITOR", "orders,users,events").split(",")]
    ANOMALY_THRESHOLD    = float(os.getenv("ANOMALY_THRESHOLD", "3.0"))
    LOOKBACK_DAYS        = int(os.getenv("LOOKBACK_DAYS", "30"))
    CHECK_INTERVAL_MINUTES = int(os.getenv("CHECK_INTERVAL_MINUTES", "60"))

    AUTO_REPAIR              = os.getenv("AUTO_REPAIR", "true").lower() == "true"
    MAX_REPAIR_RETRIES       = int(os.getenv("MAX_REPAIR_RETRIES", "3"))
    REPAIR_APPROVAL_REQUIRED = os.getenv("REPAIR_APPROVAL_REQUIRED", "false").lower() == "true"

config = Config()
