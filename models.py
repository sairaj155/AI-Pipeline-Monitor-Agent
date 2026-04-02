"""
models.py
─────────
Three tables the agent writes to:

  pipeline_snapshots  — metric profile per table per run
  anomaly_records     — every detected issue
  repair_log          — every repair action taken, with before/after proof
"""
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, Float, String,
    DateTime, Text, Boolean
)
from sqlalchemy.orm import declarative_base, sessionmaker
from src.config import config

Base = declarative_base()


class PipelineSnapshot(Base):
    __tablename__ = "pipeline_snapshots"
    id                = Column(Integer, primary_key=True, autoincrement=True)
    table_name        = Column(String(200), nullable=False)
    checked_at        = Column(DateTime, default=datetime.utcnow)
    row_count         = Column(Integer)
    row_count_delta   = Column(Integer)
    null_rate_avg     = Column(Float)
    distinct_rate_avg = Column(Float)
    column_count      = Column(Integer)
    schema_hash       = Column(String(64))
    schema_changed    = Column(Boolean, default=False)
    hours_since_update = Column(Float)


class AnomalyRecord(Base):
    __tablename__ = "anomaly_records"
    id             = Column(Integer, primary_key=True, autoincrement=True)
    detected_at    = Column(DateTime, default=datetime.utcnow)
    table_name     = Column(String(200))
    snapshot_id    = Column(Integer)
    anomaly_type   = Column(String(100))   # row_count_drop / null_spike / schema_change / stale_data
    metric_name    = Column(String(100))
    metric_value   = Column(Float)
    expected_value = Column(Float)
    z_score        = Column(Float)
    severity       = Column(String(20))    # critical / warning
    ai_diagnosis   = Column(Text)          # GPT-4o root cause explanation
    repair_chosen  = Column(String(100))   # which repair action was selected
    repair_status  = Column(String(30), default="pending")   # pending/success/failed/skipped
    alert_sent     = Column(Boolean, default=False)


class RepairLog(Base):
    """
    Records every repair the agent attempted.
    Before/after row counts prove the fix actually worked.
    """
    __tablename__ = "repair_log"
    id              = Column(Integer, primary_key=True, autoincrement=True)
    anomaly_id      = Column(Integer)
    table_name      = Column(String(200))
    repair_action   = Column(String(100))   # e.g. "reingest_missing_rows"
    repair_detail   = Column(Text)          # what exactly was done
    rows_before     = Column(Integer)
    rows_after      = Column(Integer)
    null_rate_before = Column(Float)
    null_rate_after  = Column(Float)
    started_at      = Column(DateTime, default=datetime.utcnow)
    completed_at    = Column(DateTime)
    success         = Column(Boolean)
    error_message   = Column(Text)
    verified        = Column(Boolean, default=False)  # did a re-check confirm fix?


def get_engine(url=None):
    return create_engine(url or config.AGENT_DB_URL, echo=False)

def get_session(url=None):
    engine = get_engine(url)
    return sessionmaker(bind=engine)()

def init_db():
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("✓ Agent database initialized")
