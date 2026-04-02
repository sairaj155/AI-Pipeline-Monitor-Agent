"""
profiler.py
───────────
Connects to the monitored database and computes a health profile
for each table: row counts, null rates, schema fingerprint, freshness.
"""
import hashlib
from datetime import datetime
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from rich.console import Console
from src.config import config

console = Console()


class TableProfiler:
    def __init__(self, db_url: str = None):
        self.db_url = db_url or config.MONITOR_DB_URL
        self.engine = create_engine(self.db_url)

    def profile_table(self, table_name: str) -> dict:
        console.print(f"  [cyan]Profiling[/cyan] {table_name}...")
        try:
            with self.engine.connect() as conn:
                row_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

                inspector = inspect(self.engine)
                columns   = inspector.get_columns(table_name)
                col_names = [c["name"] for c in columns]
                col_types = [str(c["type"]) for c in columns]
                schema_str  = ",".join(f"{n}:{t}" for n, t in zip(col_names, col_types))
                schema_hash = hashlib.md5(schema_str.encode()).hexdigest()

                null_rates, distinct_rates = [], []
                if row_count > 0:
                    df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 10000", self.engine)
                    for col in df.columns:
                        null_rates.append(df[col].isna().mean())
                        try:
                            distinct_rates.append(df[col].nunique() / len(df))
                        except Exception:
                            pass

                null_rate_avg     = sum(null_rates) / len(null_rates) if null_rates else 0.0
                distinct_rate_avg = sum(distinct_rates) / len(distinct_rates) if distinct_rates else 0.0

                hours_since_update = None
                ts_col = self._find_timestamp_col(col_names, col_types)
                if ts_col and row_count > 0:
                    try:
                        val = conn.execute(text(f"SELECT MAX({ts_col}) FROM {table_name}")).scalar()
                        if val:
                            if isinstance(val, str):
                                val = datetime.fromisoformat(val)
                            hours_since_update = (datetime.utcnow() - val).total_seconds() / 3600
                    except Exception:
                        pass

            return {
                "table_name": table_name, "row_count": row_count,
                "null_rate_avg": round(null_rate_avg, 4),
                "distinct_rate_avg": round(distinct_rate_avg, 4),
                "column_count": len(col_names), "schema_hash": schema_hash,
                "hours_since_update": hours_since_update,
                "col_names": col_names, "col_types": col_types, "error": None,
            }
        except Exception as e:
            console.print(f"  [red]Profile error {table_name}:[/red] {e}")
            return {"table_name": table_name, "error": str(e)}

    def profile_all(self, tables=None):
        return [self.profile_table(t.strip()) for t in (tables or config.TABLES_TO_MONITOR)]

    def _find_timestamp_col(self, names, types) -> Optional[str]:
        preferred = ["updated_at", "created_at", "timestamp", "event_time"]
        lower = [n.lower() for n in names]
        for p in preferred:
            if p in lower:
                idx = lower.index(p)
                if any(t in types[idx].upper() for t in ["DATE", "TIME", "STAMP"]):
                    return names[idx]
        for n, t in zip(names, types):
            if any(x in t.upper() for x in ["TIMESTAMP", "DATETIME"]):
                return n
        return None
