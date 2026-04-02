"""
detectors/detector.py
──────────────────────
Z-score based anomaly detection against 30-day rolling baselines.
Detects: row count drops/spikes, null rate spikes, schema changes, stale data.
"""
import numpy as np
from datetime import datetime, timedelta
from typing import Optional
from src.config import config
from src.models import PipelineSnapshot, AnomalyRecord, get_session

NUMERIC_METRICS = {
    "row_count":        {"direction": "both"},
    "null_rate_avg":    {"direction": "up"},
    "distinct_rate_avg":{"direction": "down"},
}


class AnomalyDetector:
    def __init__(self):
        self.threshold    = config.ANOMALY_THRESHOLD
        self.lookback_days = config.LOOKBACK_DAYS
        self.session      = get_session()

    def detect(self, snapshot: PipelineSnapshot) -> list[AnomalyRecord]:
        history = self._get_history(snapshot.table_name)
        if len(history) < 3:
            return []

        anomalies = []
        for metric, meta in NUMERIC_METRICS.items():
            a = self._check_metric(snapshot, history, metric, meta)
            if a:
                anomalies.append(a)

        sc = self._check_schema(snapshot, history)
        if sc:
            anomalies.append(sc)

        sf = self._check_freshness(snapshot)
        if sf:
            anomalies.append(sf)

        return anomalies

    def _get_history(self, table_name: str) -> list:
        cutoff = datetime.utcnow() - timedelta(days=self.lookback_days)
        return (self.session.query(PipelineSnapshot)
                .filter(PipelineSnapshot.table_name == table_name,
                        PipelineSnapshot.checked_at >= cutoff)
                .order_by(PipelineSnapshot.checked_at.asc()).all())

    def _check_metric(self, snap, history, metric, meta) -> Optional[AnomalyRecord]:
        vals = [getattr(s, metric) for s in history
                if getattr(s, metric) is not None and s.id != snap.id]
        if len(vals) < 3:
            return None
        current = getattr(snap, metric)
        if current is None:
            return None
        mean, std = np.mean(vals), np.std(vals)
        if std < 1e-9:
            return None
        z = (current - mean) / std
        direction = meta["direction"]
        if direction == "up"   and z <= 0: return None
        if direction == "down" and z >= 0: return None
        if abs(z) < self.threshold:        return None

        if metric == "row_count":
            atype = "row_count_drop" if z < 0 else "row_count_spike"
        elif metric == "null_rate_avg":
            atype = "null_rate_spike"
        else:
            atype = f"{metric}_anomaly"

        return AnomalyRecord(
            table_name=snap.table_name, snapshot_id=snap.id,
            anomaly_type=atype, metric_name=metric,
            metric_value=round(float(current), 4),
            expected_value=round(float(mean), 4),
            z_score=round(float(z), 2),
            severity="critical" if abs(z) >= self.threshold * 1.5 else "warning",
        )

    def _check_schema(self, snap, history) -> Optional[AnomalyRecord]:
        prev_hashes = {s.schema_hash for s in history if s.id != snap.id}
        if snap.schema_hash and prev_hashes and snap.schema_hash not in prev_hashes:
            return AnomalyRecord(
                table_name=snap.table_name, snapshot_id=snap.id,
                anomaly_type="schema_change", metric_name="schema_hash",
                metric_value=0, expected_value=0, z_score=0, severity="warning",
            )
        return None

    def _check_freshness(self, snap) -> Optional[AnomalyRecord]:
        if snap.hours_since_update and snap.hours_since_update > 24:
            return AnomalyRecord(
                table_name=snap.table_name, snapshot_id=snap.id,
                anomaly_type="stale_data", metric_name="hours_since_update",
                metric_value=round(snap.hours_since_update, 1),
                expected_value=24, z_score=0,
                severity="critical" if snap.hours_since_update > 48 else "warning",
            )
        return None
