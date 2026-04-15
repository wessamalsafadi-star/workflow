"""
db.py — SQLite persistence for campaigns and run history
"""
import sqlite3
import json
import os
from datetime import datetime
from pathlib import Path

DB_PATH = os.environ.get("CRM_DB_PATH", str(Path.home() / "crm_campaigns.db"))


def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS campaigns (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL UNIQUE,
            query_json  TEXT NOT NULL,
            ac_tag_id   INTEGER NOT NULL,
            ac_auto_id  INTEGER NOT NULL,
            drip_limit  INTEGER NOT NULL DEFAULT 100,
            start_hour  INTEGER NOT NULL DEFAULT 9,
            end_hour    INTEGER NOT NULL DEFAULT 17,
            timezone    TEXT NOT NULL DEFAULT 'Asia/Dubai',
            active      INTEGER NOT NULL DEFAULT 0,
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS run_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id     INTEGER NOT NULL,
            campaign_name   TEXT NOT NULL,
            started_at      TEXT NOT NULL,
            finished_at     TEXT,
            status          TEXT NOT NULL DEFAULT 'running',
            leads_fetched   INTEGER DEFAULT 0,
            enrolled        INTEGER DEFAULT 0,
            skipped         INTEGER DEFAULT 0,
            errors          INTEGER DEFAULT 0,
            message         TEXT,
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
        );

        CREATE TABLE IF NOT EXISTS daily_counter (
            campaign_id INTEGER NOT NULL,
            date_str    TEXT NOT NULL,
            enrolled    INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (campaign_id, date_str)
        );
        """)


def save_campaign(name, query_json, ac_tag_id, ac_auto_id,
                  drip_limit, start_hour, end_hour, timezone="Asia/Dubai",
                  campaign_id=None):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        if campaign_id:
            conn.execute("""
                UPDATE campaigns SET
                    name=?, query_json=?, ac_tag_id=?, ac_auto_id=?,
                    drip_limit=?, start_hour=?, end_hour=?, timezone=?,
                    updated_at=?
                WHERE id=?
            """, (name, json.dumps(query_json), ac_tag_id, ac_auto_id,
                  drip_limit, start_hour, end_hour, timezone, now, campaign_id))
        else:
            conn.execute("""
                INSERT INTO campaigns
                    (name, query_json, ac_tag_id, ac_auto_id,
                     drip_limit, start_hour, end_hour, timezone,
                     active, created_at, updated_at)
                VALUES (?,?,?,?,?,?,?,?,0,?,?)
            """, (name, json.dumps(query_json), ac_tag_id, ac_auto_id,
                  drip_limit, start_hour, end_hour, timezone, now, now))


def get_campaigns():
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM campaigns ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]


def get_campaign(campaign_id):
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM campaigns WHERE id=?", (campaign_id,)).fetchone()
    return dict(row) if row else None


def delete_campaign(campaign_id):
    with get_conn() as conn:
        conn.execute("DELETE FROM campaigns WHERE id=?", (campaign_id,))
        conn.execute("DELETE FROM daily_counter WHERE campaign_id=?", (campaign_id,))


def set_active(campaign_id, active: bool):
    with get_conn() as conn:
        conn.execute("UPDATE campaigns SET active=? WHERE id=?",
                     (1 if active else 0, campaign_id))


def start_run(campaign_id, campaign_name):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO run_log (campaign_id, campaign_name, started_at, status)
            VALUES (?,?,?,'running')
        """, (campaign_id, campaign_name, now))
        return cur.lastrowid


def finish_run(run_id, status, leads_fetched=0, enrolled=0,
               skipped=0, errors=0, message=""):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        conn.execute("""
            UPDATE run_log SET
                finished_at=?, status=?, leads_fetched=?,
                enrolled=?, skipped=?, errors=?, message=?
            WHERE id=?
        """, (now, status, leads_fetched, enrolled, skipped, errors, message, run_id))


def get_run_logs(campaign_id=None, limit=50):
    with get_conn() as conn:
        if campaign_id:
            rows = conn.execute("""
                SELECT * FROM run_log WHERE campaign_id=?
                ORDER BY started_at DESC LIMIT ?
            """, (campaign_id, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM run_log
                ORDER BY started_at DESC LIMIT ?
            """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def get_today_enrolled(campaign_id):
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    with get_conn() as conn:
        row = conn.execute("""
            SELECT enrolled FROM daily_counter
            WHERE campaign_id=? AND date_str=?
        """, (campaign_id, date_str)).fetchone()
    return row["enrolled"] if row else 0


def increment_today_enrolled(campaign_id, count):
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO daily_counter (campaign_id, date_str, enrolled)
            VALUES (?,?,?)
            ON CONFLICT(campaign_id, date_str)
            DO UPDATE SET enrolled = enrolled + ?
        """, (campaign_id, date_str, count, count))
