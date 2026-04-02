"""
alerts/slack_alert.py
──────────────────────
Sends two kinds of Slack messages:
  1. ISSUE DETECTED  — when anomaly is found (before repair)
  2. REPAIR COMPLETE — after repair with before/after stats + verification result
"""
import json
import httpx
from datetime import datetime
from src.config import config
from src.models import AnomalyRecord
from rich.console import Console

console = Console()


def _sev_emoji(severity): return {"critical":"🔴","warning":"🟡"}.get(severity,"⚪")
def _ok_emoji(success):   return "✅" if success else "❌"


def send_issue_alert(anomaly: AnomalyRecord, diagnosis: str, repair_chosen: str) -> bool:
    if not config.SLACK_ENABLED or not config.SLACK_WEBHOOK_URL:
        console.print("  [dim]Slack disabled — skipping alert[/dim]")
        return False

    emoji = _sev_emoji(anomaly.severity)
    blocks = [
        {"type":"header","text":{"type":"plain_text","text":f"{emoji} Pipeline anomaly detected: {anomaly.table_name}","emoji":True}},
        {"type":"section","fields":[
            {"type":"mrkdwn","text":f"*Type*\n`{anomaly.anomaly_type}`"},
            {"type":"mrkdwn","text":f"*Severity*\n{anomaly.severity.capitalize()}"},
            {"type":"mrkdwn","text":f"*Observed*\n{anomaly.metric_value:,.1f}"},
            {"type":"mrkdwn","text":f"*Expected*\n{anomaly.expected_value:,.1f}"},
            {"type":"mrkdwn","text":f"*Z-score*\n{anomaly.z_score:.1f}σ"},
            {"type":"mrkdwn","text":f"*Detected*\n{anomaly.detected_at.strftime('%H:%M UTC')}"},
        ]},
        {"type":"divider"},
        {"type":"section","text":{"type":"mrkdwn","text":f"*AI Diagnosis*\n{diagnosis}"}},
        {"type":"section","text":{"type":"mrkdwn","text":f"*Repair selected*\n`{repair_chosen}` — agent is executing now..."}},
        {"type":"context","elements":[{"type":"mrkdwn","text":"AI Pipeline Agent | Auto-repair in progress"}]},
    ]
    return _post({"blocks": blocks})


def send_repair_complete(anomaly: AnomalyRecord, repair_result, verified: bool) -> bool:
    if not config.SLACK_ENABLED or not config.SLACK_WEBHOOK_URL:
        return False

    ok    = repair_result.success
    title = f"{'✅ Repaired' if ok and verified else '⚠️ Repair attempted'}: {anomaly.table_name}"
    row_delta = repair_result.rows_after - repair_result.rows_before

    blocks = [
        {"type":"header","text":{"type":"plain_text","text":title,"emoji":True}},
        {"type":"section","fields":[
            {"type":"mrkdwn","text":f"*Repair action*\n`{repair_result.repair_action}`"},
            {"type":"mrkdwn","text":f"*Verified fixed*\n{'Yes ✅' if verified else 'No ❌'}"},
            {"type":"mrkdwn","text":f"*Rows before*\n{repair_result.rows_before:,}"},
            {"type":"mrkdwn","text":f"*Rows after*\n{repair_result.rows_after:,}"},
            {"type":"mrkdwn","text":f"*Row delta*\n{'+' if row_delta>=0 else ''}{row_delta:,}"},
            {"type":"mrkdwn","text":f"*Null rate*\n{repair_result.null_rate_before:.1%} → {repair_result.null_rate_after:.1%}"},
        ]},
        {"type":"section","text":{"type":"mrkdwn","text":f"*What was done*\n{repair_result.detail}"}},
        {"type":"context","elements":[{"type":"mrkdwn","text":
            f"Completed at {repair_result.completed_at.strftime('%H:%M UTC') if repair_result.completed_at else 'unknown'} | AI Pipeline Agent"}]},
    ]

    if repair_result.error:
        blocks.insert(2, {"type":"section","text":{"type":"mrkdwn","text":f"*Error*\n```{repair_result.error}```"}})

    return _post({"blocks": blocks})


def _post(payload: dict) -> bool:
    try:
        r = httpx.post(config.SLACK_WEBHOOK_URL,
                       content=json.dumps(payload),
                       headers={"Content-Type":"application/json"}, timeout=10)
        if r.status_code == 200:
            console.print("  [green]✓ Slack alert sent[/green]")
            return True
        console.print(f"  [red]Slack error {r.status_code}[/red]")
        return False
    except Exception as e:
        console.print(f"  [red]Slack failed:[/red] {e}")
        return False
