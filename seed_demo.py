"""
seed_demo.py
────────────
Creates a fully realistic demo database with 3 tables and 30 days
of historical snapshots. Supports multiple injectable anomaly types.

Usage:
  python seed_demo.py                          # fresh start (no anomaly)
  python seed_demo.py --anomaly row_drop       # delete 94% of orders rows
  python seed_demo.py --anomaly null_spike     # corrupt 60% of user emails to NULL
  python seed_demo.py --anomaly duplicate_rows # duplicate all events rows
  python seed_demo.py --anomaly schema_change  # rename a column in users
  python seed_demo.py --anomaly stale_data     # set all timestamps to 48h ago
"""
import os, sys, random, sqlite3
from datetime import datetime, timedelta
from rich.console import Console
from src.models import init_db, PipelineSnapshot, get_session
from src.config import config

console = Console()
random.seed(42)
DB_PATH = "data/pipeline.db"


def create_tables():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c    = conn.cursor()

    c.execute("DROP TABLE IF EXISTS orders")
    c.execute("""CREATE TABLE orders (
        id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL,
        amount REAL NOT NULL, status TEXT NOT NULL,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")

    c.execute("DROP TABLE IF EXISTS users")
    c.execute("""CREATE TABLE users (
        id INTEGER PRIMARY KEY, email TEXT NOT NULL,
        name TEXT, country TEXT, created_at TEXT NOT NULL)""")

    c.execute("DROP TABLE IF EXISTS events")
    c.execute("""CREATE TABLE events (
        id INTEGER PRIMARY KEY, user_id INTEGER,
        event_type TEXT NOT NULL, payload TEXT, created_at TEXT NOT NULL)""")

    now = datetime.utcnow()

    # Orders — ~19,000 rows
    orders = []
    for i in range(19_200):
        ts = (now - timedelta(hours=random.uniform(0,2))).strftime("%Y-%m-%d %H:%M:%S")
        orders.append((i+1, random.randint(1,5000), round(random.uniform(5,500),2),
                       random.choice(["completed","pending","refunded"]), ts, ts))
    c.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", orders)

    # Users — 5,000 rows
    users = []
    for i in range(5000):
        ts = (now - timedelta(days=random.randint(0,365))).strftime("%Y-%m-%d %H:%M:%S")
        users.append((i+1, f"user{i}@example.com", f"User {i}",
                      random.choice(["US","UK","DE","FR","CA"]), ts))
    c.executemany("INSERT INTO users VALUES (?,?,?,?,?)", users)

    # Events — 80,000 rows
    events = []
    for i in range(80_000):
        ts = (now - timedelta(hours=random.uniform(0,3))).strftime("%Y-%m-%d %H:%M:%S")
        events.append((i+1, random.randint(1,5000),
                       random.choice(["page_view","click","purchase","signup"]),
                       '{"key":"value"}', ts))
    c.executemany("INSERT INTO events VALUES (?,?,?,?,?)", events)

    conn.commit()
    conn.close()
    console.print(f"[green]✓ Pipeline DB created:[/green] {DB_PATH}")
    console.print("  orders: 19,200 | users: 5,000 | events: 80,000")


def seed_snapshots():
    console.print("\n[cyan]Seeding 30 days of historical snapshots...[/cyan]")
    init_db()
    session = get_session()
    session.query(PipelineSnapshot).delete()
    session.commit()

    now = datetime.utcnow()
    schema_hashes = {"orders":"abc111","users":"abc222","events":"abc333"}

    profiles = {
        "orders": {"rc_mean":19000,"rc_std":400,"nr_mean":0.02,"nr_std":0.005,"cols":6},
        "users":  {"rc_mean":5000, "rc_std":50, "nr_mean":0.05,"nr_std":0.01, "cols":5},
        "events": {"rc_mean":80000,"rc_std":3000,"nr_mean":0.01,"nr_std":0.003,"cols":5},
    }

    snaps = []
    for day in range(30, 0, -1):
        t = now - timedelta(days=day, hours=random.uniform(0,1))
        for tbl, p in profiles.items():
            snaps.append(PipelineSnapshot(
                table_name=tbl, checked_at=t,
                row_count=max(1,int(random.gauss(p["rc_mean"],p["rc_std"]))),
                null_rate_avg=max(0,random.gauss(p["nr_mean"],p["nr_std"])),
                distinct_rate_avg=max(0,random.gauss(0.85,0.02)),
                column_count=p["cols"],
                schema_hash=schema_hashes[tbl],
                hours_since_update=random.uniform(0.5,3),
            ))

    session.add_all(snaps)
    session.commit()
    console.print(f"[green]✓ Seeded {len(snaps)} snapshots[/green]")


# ── Anomaly injectors ──────────────────────────────────────────────────────────

def inject_row_drop():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM orders WHERE id > 1200")
    remaining = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    conn.commit(); conn.close()
    console.print(f"[red]✓ Injected ROW DROP:[/red] orders has {remaining:,} rows (was 19,200)")

def inject_null_spike():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("UPDATE users SET email = NULL WHERE id % 2 = 0")
    conn.execute("UPDATE users SET name  = NULL WHERE id % 3 = 0")
    conn.commit(); conn.close()
    console.print("[red]✓ Injected NULL SPIKE:[/red] ~60% of user emails/names set to NULL")

def inject_duplicate_rows():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT INTO events (user_id, event_type, payload, created_at)
        SELECT user_id, event_type, payload, created_at FROM events LIMIT 80000
    """)
    total = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    conn.commit(); conn.close()
    console.print(f"[red]✓ Injected DUPLICATES:[/red] events now has {total:,} rows (was 80,000)")

def inject_schema_change():
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("ALTER TABLE users ADD COLUMN user_score REAL")
        conn.commit()
        console.print("[red]✓ Injected SCHEMA CHANGE:[/red] added 'user_score' column to users")
    except Exception:
        console.print("[yellow]Schema change already applied[/yellow]")
    conn.close()

def inject_stale_data():
    stale_ts = (datetime.utcnow() - timedelta(hours=50)).strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect(DB_PATH)
    conn.execute(f"UPDATE orders SET updated_at = '{stale_ts}'")
    conn.commit(); conn.close()
    console.print("[red]✓ Injected STALE DATA:[/red] all order timestamps set 50 hours ago")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    anomaly = None
    for arg in sys.argv[1:]:
        if arg.startswith("--anomaly="):
            anomaly = arg.split("=")[1]
        elif arg == "--anomaly" and len(sys.argv) > sys.argv.index(arg) + 1:
            anomaly = sys.argv[sys.argv.index(arg) + 1]

    create_tables()
    seed_snapshots()

    injectors = {
        "row_drop":      inject_row_drop,
        "null_spike":    inject_null_spike,
        "duplicate_rows":inject_duplicate_rows,
        "schema_change": inject_schema_change,
        "stale_data":    inject_stale_data,
    }

    if anomaly:
        if anomaly in injectors:
            injectors[anomaly]()
        else:
            console.print(f"[red]Unknown anomaly: {anomaly}[/red]")
            console.print(f"Available: {', '.join(injectors.keys())}")
    else:
        console.print("\n[dim]Tip: add --anomaly=row_drop to inject a failure[/dim]")

    console.print(f"\n[bold green]Ready![/bold green] Now run: python run_agent.py --once")
