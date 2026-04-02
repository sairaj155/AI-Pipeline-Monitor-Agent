import numpy as np
from src.models import AnomalyRecord, PipelineSnapshot, get_session
from src.profiler import TableProfiler
from src.config import config
from rich.console import Console
 
console = Console()
 
NUMERIC_METRICS = {"row_count", "null_rate_avg", "distinct_rate_avg", "hours_since_update"}
 
 
class RepairVerifier:
    def __init__(self):
        self.profiler = TableProfiler()
        self.session  = get_session()
 
    def verify(self, anomaly: AnomalyRecord, repair_result) -> bool:
        console.print(f"  [cyan]Verifying repair...[/cyan]")
 
        if anomaly.metric_name not in NUMERIC_METRICS:
            resolved = repair_result.success
            status   = "[green]VERIFIED ✓[/green]" if resolved else "[yellow]UNRESOLVED[/yellow]"
            console.print(f"  {status} (non-numeric metric — verified by repair success flag)")
            return resolved
 
        fresh = self.profiler.profile_table(anomaly.table_name)
        if fresh.get("error"):
            console.print(f"  [red]Verification failed — could not profile table[/red]")
            return False
 
        metric  = anomaly.metric_name
        current = fresh.get(metric)
 
        if current is None:
            console.print(f"  [green]VERIFIED ✓[/green] (metric no longer present)")
            return True
 
        from datetime import datetime, timedelta
        cutoff  = datetime.utcnow() - timedelta(days=config.LOOKBACK_DAYS)
        history = (self.session.query(PipelineSnapshot)
                   .filter(PipelineSnapshot.table_name == anomaly.table_name,
                           PipelineSnapshot.checked_at >= cutoff)
                   .all())
 
        hist_vals = []
        for s in history:
            val = getattr(s, metric, None)
            if val is not None:
                try:
                    hist_vals.append(float(val))
                except (TypeError, ValueError):
                    pass
 
        if len(hist_vals) < 3:
            if metric == "row_count":
                resolved = repair_result.rows_after > repair_result.rows_before
            elif metric == "null_rate_avg":
                resolved = repair_result.null_rate_after < repair_result.null_rate_before
            else:
                resolved = repair_result.success
        else:
            mean = np.mean(hist_vals)
            std  = np.std(hist_vals)
            if std < 1e-9:
                resolved = True
            else:
                new_z    = abs((float(current) - mean) / std)
                resolved = new_z < 1.5
 
        status = "[green]VERIFIED ✓[/green]" if resolved else "[yellow]STILL ANOMALOUS[/yellow]"
        console.print(f"  Verification: {status}")
        console.print(f"  Current {metric}: {current} (was {anomaly.metric_value})")
        return resolved
