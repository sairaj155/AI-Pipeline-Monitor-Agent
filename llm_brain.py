"""
llm/llm_brain.py
-----------------
Does two things:
  1. DIAGNOSE  — explains what broke and why
  2. DECIDE    — picks the best repair action

When an OpenAI/Anthropic key IS set:  uses GPT-4o / Claude
When no key is set:                   uses smart rule-based fallback
                                      (still picks the RIGHT repair, just
                                       without the natural-language explanation)
"""
import json
from src.config import config
from src.models import AnomalyRecord, PipelineSnapshot

REPAIR_ACTIONS = [
    "reingest_missing_rows",
    "repair_null_values",
    "remove_duplicate_rows",
    "rollback_schema_change",
    "refresh_stale_table",
    "quarantine_bad_rows",
    "no_action",
]

# Rule-based repair selection — works without any API key
ANOMALY_REPAIR_MAP = {
    "row_count_drop":         "reingest_missing_rows",
    "row_count_spike":        "remove_duplicate_rows",
    "null_rate_spike":        "repair_null_values",
    "null_spike":             "repair_null_values",
    "distinct_rate_avg_anomaly": "repair_null_values",
    "schema_change":          "rollback_schema_change",
    "stale_data":             "refresh_stale_table",
}

ANOMALY_DIAGNOSES = {
    "row_count_drop": (
        "The table row count dropped significantly below the 30-day baseline. "
        "This typically means a failed or truncated ingestion job, a missing "
        "upstream data source, or an accidental DELETE. Re-ingesting the missing "
        "rows for the affected time window is the correct repair."
    ),
    "row_count_spike": (
        "The table row count spiked far above baseline. This usually indicates "
        "a duplicate ingestion run, removal of deduplication logic, or an upstream "
        "system replaying historical data. Removing duplicate rows is the correct repair."
    ),
    "null_rate_spike": (
        "The null rate spiked above the historical average, meaning many columns "
        "that normally have values are now NULL. This is often caused by a schema "
        "change in the source system, a broken transformation, or a failed JOIN. "
        "Filling NULLs with column medians or modes is the correct repair."
    ),
    "distinct_rate_avg_anomaly": (
        "The average distinct value rate dropped below baseline, suggesting many "
        "columns now have reduced cardinality — often caused by NULL flooding or "
        "a data type change upstream. Repairing null values will restore cardinality."
    ),
    "schema_change": (
        "The table schema fingerprint changed since the last run. A column was "
        "likely added, removed, or renamed. This can silently break downstream "
        "models. The schema change has been logged for engineer review."
    ),
    "stale_data": (
        "The table has not received fresh data within the expected SLA window. "
        "The scheduled ingestion job may have failed or been skipped. "
        "Refreshing the table's timestamp and triggering a reload is the correct repair."
    ),
}


def build_prompt(anomaly: AnomalyRecord, snapshot: PipelineSnapshot, history: list) -> str:
    recent_vals = [str(getattr(s, anomaly.metric_name, "N/A")) for s in history[-7:]]
    history_str = ", ".join(recent_vals) if recent_vals else "no history"

    return f"""You are an autonomous AI data engineering agent. A pipeline anomaly has been detected.
Respond ONLY with valid JSON — no markdown, no extra text.

ANOMALY
-------
Table:        {anomaly.table_name}
Type:         {anomaly.anomaly_type}
Metric:       {anomaly.metric_name}
Observed:     {anomaly.metric_value}
Expected:     {anomaly.expected_value}
Z-score:      {anomaly.z_score}
Severity:     {anomaly.severity}

TABLE STATE
-----------
Row count:    {snapshot.row_count:,}
Null rate:    {snapshot.null_rate_avg:.1%}
Columns:      {snapshot.column_count}
Hours stale:  {snapshot.hours_since_update or 'unknown'}
Last 7 readings of {anomaly.metric_name}: {history_str}

REPAIR OPTIONS (pick exactly one):
  reingest_missing_rows   - re-fills missing data rows
  repair_null_values      - fills NULLs with median/mode
  remove_duplicate_rows   - removes duplicate records
  rollback_schema_change  - logs and flags schema drift
  refresh_stale_table     - resets freshness timestamp
  quarantine_bad_rows     - isolates corrupt rows
  no_action               - log only, no DB change

Respond with exactly this JSON:
{{
  "diagnosis": "2-3 sentence plain English root cause",
  "repair_action": "one action from the list above",
  "repair_reason": "one sentence why",
  "confidence": 0.0-1.0,
  "estimated_rows_recovered": number_or_null
}}"""


def call_llm(prompt: str) -> dict:
    raw = ""
    try:
        if config.LLM_PROVIDER == "anthropic" and config.ANTHROPIC_API_KEY:
            import anthropic
            client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
            resp   = client.messages.create(
                model="claude-sonnet-4-6", max_tokens=600,
                messages=[{"role": "user", "content": prompt}]
            )
            raw = resp.content[0].text.strip()
            model_used = "claude-sonnet-4-6"

        elif config.OPENAI_API_KEY:
            from openai import OpenAI
            client = OpenAI(api_key=config.OPENAI_API_KEY)
            resp   = client.chat.completions.create(
                model="gpt-4o", max_tokens=600, temperature=0.2,
                messages=[
                    {"role": "system", "content": "You are a data engineering AI agent. Always respond with valid JSON only."},
                    {"role": "user",   "content": prompt},
                ]
            )
            raw = resp.choices[0].message.content.strip()
            model_used = "gpt-4o"
        else:
            return None   # signal to use rule-based fallback

        # Strip markdown fences if present
        if "```" in raw:
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]

        result = json.loads(raw.strip())
        result["model_used"] = model_used
        return result

    except Exception as e:
        return None   # fall through to rule-based


def rule_based_decision(anomaly: AnomalyRecord) -> dict:
    """
    Smart rule-based fallback — picks the correct repair action
    based on the anomaly type, without needing any API key.
    """
    repair = ANOMALY_REPAIR_MAP.get(anomaly.anomaly_type, "no_action")
    diagnosis = ANOMALY_DIAGNOSES.get(
        anomaly.anomaly_type,
        f"Statistical anomaly detected: {anomaly.metric_name} = {anomaly.metric_value} "
        f"(expected ~{anomaly.expected_value}, Z={anomaly.z_score:.1f}). "
        f"Rule-based repair selected — add an OpenAI key for AI diagnosis."
    )

    pct = 0.0
    if anomaly.expected_value and anomaly.expected_value != 0:
        pct = abs((anomaly.metric_value - anomaly.expected_value) / anomaly.expected_value) * 100

    estimated = None
    if repair == "reingest_missing_rows" and anomaly.metric_name == "row_count":
        estimated = int(anomaly.expected_value - anomaly.metric_value)

    return {
        "diagnosis":   diagnosis,
        "repair_action": repair,
        "repair_reason": f"Rule-based selection for anomaly type '{anomaly.anomaly_type}' "
                         f"({pct:.0f}% deviation from baseline).",
        "confidence":  0.80,
        "estimated_rows_recovered": estimated,
        "model_used":  "rule-based",
    }


class LLMBrain:
    def diagnose_and_decide(
        self,
        anomaly: AnomalyRecord,
        snapshot: PipelineSnapshot,
        history: list,
    ) -> dict:
        prompt = build_prompt(anomaly, snapshot, history)
        result = call_llm(prompt)

        if result is None:
            # No API key or LLM call failed — use smart rule-based fallback
            result = rule_based_decision(anomaly)

        # Safety check — ensure repair action is one we know
        if result.get("repair_action") not in REPAIR_ACTIONS:
            result["repair_action"] = ANOMALY_REPAIR_MAP.get(anomaly.anomaly_type, "no_action")

        return result
