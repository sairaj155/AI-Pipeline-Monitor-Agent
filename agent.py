"""
agent.py
─────────
The main AI agent loop. Orchestrates all 4 steps:

  STEP 1 — DETECT    : profile tables, find anomalies
  STEP 2 — DIAGNOSE  : LLM explains root cause + chooses repair
  STEP 3 — REPAIR    : execute the repair action on the live database
  STEP 4 — VERIFY    : re-check to confirm the fix worked
  STEP 5 — REPORT    : Slack alert + dashboard update

Run once:       python run_agent.py --once
Run continuous: python run_agent.py
"""
from datetime import datetime
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.config import config
from src.models import init_db, PipelineSnapshot, AnomalyRecord, RepairLog, get_session
from src.profiler import TableProfiler
from src.detectors.detector import AnomalyDetector
from src.llm.llm_brain import LLMBrain
from src.repairs.repair_engine import RepairEngine
from src.repairs.verifier import RepairVerifier
from src.alerts.slack_alert import send_issue_alert, send_repair_complete

console = Console()


class PipelineAgent:
    def __init__(self):
        self.profiler  = TableProfiler()
        self.detector  = AnomalyDetector()
        self.brain     = LLMBrain()
        self.repairer  = RepairEngine()
        self.verifier  = RepairVerifier()
        self.session   = get_session()

    def run(self) -> dict:
        """Execute one full agent cycle. Returns a summary dict."""
        run_start = datetime.utcnow()
        console.print(Panel(
            f"[bold]AI Pipeline Agent[/bold] — {run_start.strftime('%Y-%m-%d %H:%M')} UTC\n"
            f"Mode: {'AUTO-REPAIR' if config.AUTO_REPAIR else 'DETECT-ONLY'} | "
            f"Tables: {', '.join(config.TABLES_TO_MONITOR)}",
            style="bold blue"
        ))

        total_anomalies = 0
        total_repaired  = 0
        total_verified  = 0
        table_results   = []

        for table_name in config.TABLES_TO_MONITOR:
            table_name = table_name.strip()
            console.print(f"\n[bold]━━ {table_name} ━━[/bold]")
            result = self._process_table(table_name)
            table_results.append(result)
            total_anomalies += result["anomalies"]
            total_repaired  += result["repaired"]
            total_verified  += result["verified"]

        duration = (datetime.utcnow() - run_start).total_seconds()
        summary  = {
            "run_at":           run_start.isoformat(),
            "tables_checked":   len(config.TABLES_TO_MONITOR),
            "anomalies_found":  total_anomalies,
            "repairs_attempted":total_repaired,
            "repairs_verified": total_verified,
            "duration_seconds": round(duration, 1),
            "table_results":    table_results,
        }

        self._print_summary(summary)
        return summary

    def _process_table(self, table_name: str) -> dict:
        result = {"table": table_name, "anomalies": 0, "repaired": 0, "verified": 0}

        # ── STEP 1: DETECT ───────────────────────────────────────────────────
        metrics = self.profiler.profile_table(table_name)
        if metrics.get("error"):
            console.print(f"  [red]Cannot profile {table_name}: {metrics['error']}[/red]")
            return result

        snapshot = self._save_snapshot(metrics)
        anomalies = self.detector.detect(snapshot)

        if not anomalies:
            console.print(f"  [green]✓ All metrics normal[/green]")
            return result

        result["anomalies"] = len(anomalies)
        console.print(f"  [yellow]Found {len(anomalies)} anomaly(ies)[/yellow]")

        history = self.detector._get_history(table_name)

        for anomaly in anomalies:
            anomaly.detected_at = datetime.utcnow()
            self.session.add(anomaly)
            self.session.flush()

            self._print_anomaly(anomaly)

            # ── STEP 2: DIAGNOSE ─────────────────────────────────────────────
            console.print(f"\n  [bold cyan]STEP 2 — AI Diagnosis[/bold cyan]")
            decision = self.brain.diagnose_and_decide(anomaly, snapshot, history)

            anomaly.ai_diagnosis  = decision["diagnosis"]
            anomaly.repair_chosen = decision["repair_action"]
            self.session.commit()

            console.print(f"  [magenta]Diagnosis:[/magenta] {decision['diagnosis']}")
            console.print(f"  [magenta]Repair chosen:[/magenta] {decision['repair_action']} "
                          f"(confidence: {decision.get('confidence', 0):.0%})")
            console.print(f"  [magenta]Reason:[/magenta] {decision.get('repair_reason', '')}")

            # ── STEP 3: REPAIR ───────────────────────────────────────────────
            if not config.AUTO_REPAIR:
                console.print("  [dim]AUTO_REPAIR=false — skipping repair[/dim]")
                anomaly.repair_status = "skipped"
                self.session.commit()
                continue

            # Send "issue detected" alert BEFORE repair starts
            send_issue_alert(anomaly, decision["diagnosis"], decision["repair_action"])

            console.print(f"\n  [bold cyan]STEP 3 — Executing Repair[/bold cyan]")
            repair_result = self.repairer.execute(
                repair_action=decision["repair_action"],
                table_name=table_name,
                anomaly_context={
                    "anomaly_type":   anomaly.anomaly_type,
                    "metric_value":   anomaly.metric_value,
                    "expected_value": anomaly.expected_value,
                    "z_score":        anomaly.z_score,
                }
            )
            result["repaired"] += 1

            # Save repair log
            log = RepairLog(
                anomaly_id       = anomaly.id,
                table_name       = table_name,
                repair_action    = repair_result.repair_action,
                repair_detail    = repair_result.detail,
                rows_before      = repair_result.rows_before,
                rows_after       = repair_result.rows_after,
                null_rate_before = repair_result.null_rate_before,
                null_rate_after  = repair_result.null_rate_after,
                started_at       = repair_result.started_at,
                completed_at     = repair_result.completed_at,
                success          = repair_result.success,
                error_message    = repair_result.error,
            )
            self.session.add(log)

            # ── STEP 4: VERIFY ───────────────────────────────────────────────
            console.print(f"\n  [bold cyan]STEP 4 — Verifying Fix[/bold cyan]")
            verified = self.verifier.verify(anomaly, repair_result)
            log.verified = verified
            anomaly.repair_status = "success" if (repair_result.success and verified) else (
                "partial" if repair_result.success else "failed"
            )
            self.session.commit()

            if verified:
                result["verified"] += 1

            # ── STEP 5: REPORT ───────────────────────────────────────────────
            console.print(f"\n  [bold cyan]STEP 5 — Sending Report[/bold cyan]")
            sent = send_repair_complete(anomaly, repair_result, verified)
            anomaly.alert_sent = sent
            self.session.commit()

        return result

    def _save_snapshot(self, metrics: dict) -> PipelineSnapshot:
        prev = (self.session.query(PipelineSnapshot)
                .filter_by(table_name=metrics["table_name"])
                .order_by(PipelineSnapshot.checked_at.desc()).first())

        row_delta      = None
        schema_changed = False
        if prev:
            if metrics.get("row_count") is not None and prev.row_count is not None:
                row_delta = metrics["row_count"] - prev.row_count
            if prev.schema_hash and metrics.get("schema_hash"):
                schema_changed = prev.schema_hash != metrics["schema_hash"]

        snap = PipelineSnapshot(
            table_name        = metrics["table_name"],
            checked_at        = datetime.utcnow(),
            row_count         = metrics.get("row_count"),
            row_count_delta   = row_delta,
            null_rate_avg     = metrics.get("null_rate_avg"),
            distinct_rate_avg = metrics.get("distinct_rate_avg"),
            column_count      = metrics.get("column_count"),
            schema_hash       = metrics.get("schema_hash"),
            schema_changed    = schema_changed,
            hours_since_update= metrics.get("hours_since_update"),
        )
        self.session.add(snap)
        self.session.commit()
        return snap

    def _print_anomaly(self, anomaly: AnomalyRecord):
        console.print(f"\n  [bold cyan]STEP 1 — Anomaly Detected[/bold cyan]")
        t = Table(show_header=False, box=None, padding=(0,2))
        t.add_row("Type",     f"[red]{anomaly.anomaly_type}[/red]")
        t.add_row("Severity", f"[{'red' if anomaly.severity=='critical' else 'yellow'}]{anomaly.severity}[/]")
        t.add_row("Metric",   anomaly.metric_name)
        t.add_row("Observed", f"{anomaly.metric_value:,.2f}")
        t.add_row("Expected", f"{anomaly.expected_value:,.2f}")
        t.add_row("Z-score",  f"{anomaly.z_score:.2f}σ")
        console.print(t)

    def _print_summary(self, summary: dict):
        console.print(Panel(
            f"[bold]Run complete[/bold]\n"
            f"Tables: {summary['tables_checked']} | "
            f"Anomalies: {summary['anomalies_found']} | "
            f"Repaired: {summary['repairs_attempted']} | "
            f"Verified: {summary['repairs_verified']} | "
            f"Time: {summary['duration_seconds']}s",
            style="green" if summary['anomalies_found'] == 0 else "yellow"
        ))
