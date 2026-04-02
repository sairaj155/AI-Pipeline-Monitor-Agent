# 🤖 AI Pipeline Agent

An autonomous AI agent that **detects**, **diagnoses**, **repairs**, and **verifies** data pipeline failures — automatically, without human intervention.

> This is not just a monitor. It's an agent that fixes the problem.

---

## What makes this different

| Traditional monitor | This AI agent |
|---|---|
| "Row count dropped" | Diagnoses root cause with GPT-4o |
| Sends an alert | Executes a repair action on the database |
| You fix it manually | Verifies the fix actually worked |
| Alert every run | Sends before + after report to Slack |

---

## What it actually does — step by step

```
STEP 1 — DETECT
  Every hour, profiles each table:
  row counts · null rates · schema fingerprint · data freshness
  Flags anomalies using Z-score (3σ = 0.3% false positive rate)

STEP 2 — DIAGNOSE
  Sends anomaly context to GPT-4o
  GPT-4o returns: root cause + repair action choice + confidence

STEP 3 — REPAIR (automatically executes one of):
  reingest_missing_rows   → re-fills missing data rows
  repair_null_values      → fills NULLs with median/mode
  remove_duplicate_rows   → deduplicates keeping latest record
  rollback_schema_change  → logs and flags schema drift
  refresh_stale_table     → resets freshness timestamp
  quarantine_bad_rows     → isolates corrupt rows safely
  no_action               → logs + alerts without touching data

STEP 4 — VERIFY
  Re-profiles the table after repair
  Checks if anomaly metric is back within normal range (Z < 1.5)
  Reports: verified ✅ or still anomalous ⚠️

STEP 5 — REPORT
  Slack: issue alert (before repair) + repair outcome (after)
  Dashboard: all results visible in real time
```

---

## Project structure

```
pipeline_agent/
├── run_agent.py              ← MAIN FILE — run this
├── seed_demo.py              ← creates demo data + injects anomalies
├── requirements.txt
├── .env.example
├── src/
│   ├── config.py             ← all settings from .env
│   ├── models.py             ← DB schema: snapshots, anomalies, repair_log
│   ├── profiler.py           ← measures table health
│   ├── agent/
│   │   └── agent.py          ← main orchestrator (steps 1-5)
│   ├── detectors/
│   │   └── detector.py       ← Z-score anomaly detection
│   ├── llm/
│   │   └── llm_brain.py      ← GPT-4o diagnosis + repair decision
│   ├── repairs/
│   │   ├── repair_engine.py  ← executes all repair actions
│   │   └── verifier.py       ← confirms fix worked
│   └── alerts/
│       └── slack_alert.py    ← Slack issue + repair alerts
├── dashboard/
│   └── app.py                ← Streamlit dashboard
└── tests/
    └── test_agent.py         ← 9 automated tests
```

---

## Step-by-step: run it locally

### Step 1 — Extract and enter the project

```bash
tar -xf pipeline_agent.tar
cd pipeline_agent
```

### Step 2 — Create virtual environment

```bash
python -m venv venv
source venv/bin/activate        # Mac/Linux
# venv\Scripts\activate         # Windows
```

### Step 3 — Install dependencies

```bash
pip install -r requirements.txt
```

### Step 4 — Configure

```bash
cp .env.example .env
```

Open `.env` and set your OpenAI key:
```
OPENAI_API_KEY=sk-your-key-here
```

Everything else works with the defaults for local testing.

### Step 5 — Create demo data (normal, healthy state)

```bash
python seed_demo.py
```

Creates 3 tables (orders: 19,200 rows, users: 5,000, events: 80,000)
and 30 days of historical snapshots for the baseline.

### Step 6 — Run agent on healthy data (should show all clear)

```bash
python run_agent.py --once
```

Expected output:
```
━━ orders ━━
  Profiling orders...
  ✓ All metrics normal

━━ users ━━
  ✓ All metrics normal

━━ events ━━
  ✓ All metrics normal

Run complete | 3 tables | 0 anomalies | 0 repairs | 1.2s
```

### Step 7 — Inject an anomaly

Pick one of 5 anomaly types:

```bash
# Delete 94% of orders rows
python seed_demo.py --anomaly=row_drop

# Corrupt 60% of user emails to NULL
python seed_demo.py --anomaly=null_spike

# Duplicate all events rows
python seed_demo.py --anomaly=duplicate_rows

# Add an unexpected column to users
python seed_demo.py --anomaly=schema_change

# Set all timestamps 50 hours ago (stale data)
python seed_demo.py --anomaly=stale_data
```

### Step 8 — Run the agent again (watch it detect, diagnose, and fix)

```bash
python run_agent.py --once
```

Example output for `row_drop`:
```
━━ orders ━━
  Profiling orders...
  Found 1 anomaly(ies)

  STEP 1 — Anomaly Detected
    Type      row_count_drop
    Severity  critical
    Observed  1,200.00
    Expected  19,000.00
    Z-score   -44.50σ

  STEP 2 — AI Diagnosis
    Diagnosis:    The orders table loaded 1,200 rows vs expected 19,000.
                  This 94% drop is consistent with a truncated ingestion job
                  or a missing upstream data source connection.
    Repair chosen: reingest_missing_rows (confidence: 92%)
    Reason:       Row count drop with no schema change indicates missing data,
                  best resolved by re-ingesting the affected window.

  STEP 3 — Executing Repair
    Re-ingested 18,000 missing rows into orders.
    Rows: 1,200 → 19,200 (+18,000)
    Null rate: 2.0% → 2.0%
    Status: SUCCESS

  STEP 4 — Verifying Fix
    Current row_count: 19,200 (was 1,200)
    Verification: VERIFIED ✓

  STEP 5 — Sending Report
    ✓ Slack alert sent

Run complete | 3 tables | 1 anomaly | 1 repair | 1 verified | 4.8s
```

### Step 9 — Open the dashboard

```bash
streamlit run dashboard/app.py
```

Open http://localhost:8501 — you'll see the anomaly card with AI diagnosis, repair outcome, before/after row counts, and verification status.

### Step 10 — Run tests

```bash
pytest tests/ -v
```

---

## Deploy online for free

### Streamlit Cloud (recommended — 5 minutes)

1. Push to GitHub:
```bash
git init
git add .
git commit -m "initial commit"
git remote add origin https://github.com/YOUR_USERNAME/pipeline-agent
git push -u origin main
```

2. Go to [share.streamlit.io](https://share.streamlit.io) → New app
3. Select your repo → Main file: `dashboard/app.py`
4. Click **Advanced settings → Secrets** and add:
```toml
OPENAI_API_KEY = "sk-your-key"
MONITOR_DB_URL = "sqlite:///./data/pipeline.db"
AGENT_DB_URL   = "sqlite:///./data/agent.db"
TABLES_TO_MONITOR = "orders,users,events"
SLACK_ENABLED = "false"
AUTO_REPAIR = "true"
```
5. Deploy → get a live URL like `yourname-pipeline-agent.streamlit.app`

For persistent storage across restarts, use a free Postgres from [supabase.com](https://supabase.com):
```toml
AGENT_DB_URL = "postgresql://user:pass@host:5432/dbname"
```

---

## Connect to your real database

Just change `MONITOR_DB_URL` in `.env`:

```bash
# Postgres
MONITOR_DB_URL=postgresql://user:password@host:5432/mydb

# MySQL
MONITOR_DB_URL=mysql+pymysql://user:password@host:3306/mydb

# Snowflake (pip install snowflake-sqlalchemy)
MONITOR_DB_URL=snowflake://user:password@account/database/schema
```

---

## Set up Slack alerts

1. Go to [api.slack.com/apps](https://api.slack.com/apps) → Create New App
2. Enable Incoming Webhooks → Add to workspace → pick channel
3. Copy the webhook URL into `.env`:
```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXX/YYY/ZZZ
SLACK_ENABLED=true
```

You'll get two messages per incident:
- **Before repair**: what broke, AI diagnosis, repair action chosen
- **After repair**: rows before/after, null rate change, verified status

---

## Run on a schedule

```bash
# Continuous (every N minutes from config)
python run_agent.py

# Cron (every hour)
0 * * * * cd /path/to/pipeline_agent && python run_agent.py --once

# Detect only (no auto-repair)
python run_agent.py --detect-only
```

---

## Tech stack

| Component | Tool |
|---|---|
| Language | Python 3.11 |
| Statistical detection | NumPy Z-score |
| AI diagnosis + repair decision | OpenAI GPT-4o |
| Database connectivity | SQLAlchemy (any SQL DB) |
| Data profiling | Pandas |
| Alerting | Slack Incoming Webhooks |
| Dashboard | Streamlit |
| Storage | SQLite (dev) / PostgreSQL (prod) |
| Scheduling | schedule / cron |
| Testing | pytest |

---

## License

MIT — free to use, showcase, and build on.
