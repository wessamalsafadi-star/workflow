"""
db.py — Postgres persistence for campaigns and run history
Connects via DATABASE_URL environment variable (Supabase / any Postgres).
"""
import json
import os
from datetime import datetime
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL", "")


@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS campaigns (
                id          SERIAL PRIMARY KEY,
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
                id              SERIAL PRIMARY KEY,
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
        with conn.cursor() as cur:
            if campaign_id:
                cur.execute("""
                    UPDATE campaigns SET
                        name=%s, query_json=%s, ac_tag_id=%s, ac_auto_id=%s,
                        drip_limit=%s, start_hour=%s, end_hour=%s, timezone=%s,
                        updated_at=%s
                    WHERE id=%s
                """, (name, json.dumps(query_json), ac_tag_id, ac_auto_id,
                      drip_limit, start_hour, end_hour, timezone, now, campaign_id))
            else:
                cur.execute("""
                    INSERT INTO campaigns
                        (name, query_json, ac_tag_id, ac_auto_id,
                         drip_limit, start_hour, end_hour, timezone,
                         active, created_at, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s)
                """, (name, json.dumps(query_json), ac_tag_id, ac_auto_id,
                      drip_limit, start_hour, end_hour, timezone, now, now))


def get_campaigns():
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM campaigns ORDER BY created_at DESC")
            return [dict(r) for r in cur.fetchall()]


def get_campaign(campaign_id):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM campaigns WHERE id=%s", (campaign_id,))
            row = cur.fetchone()
    return dict(row) if row else None


def delete_campaign(campaign_id):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM campaigns WHERE id=%s", (campaign_id,))
            cur.execute("DELETE FROM daily_counter WHERE campaign_id=%s", (campaign_id,))


def set_active(campaign_id, active: bool):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE campaigns SET active=%s WHERE id=%s",
                        (1 if active else 0, campaign_id))


def start_run(campaign_id, campaign_name):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO run_log (campaign_id, campaign_name, started_at, status)
                VALUES (%s,%s,%s,'running')
                RETURNING id
            """, (campaign_id, campaign_name, now))
            return cur.fetchone()[0]


def finish_run(run_id, status, leads_fetched=0, enrolled=0,
               skipped=0, errors=0, message=""):
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE run_log SET
                    finished_at=%s, status=%s, leads_fetched=%s,
                    enrolled=%s, skipped=%s, errors=%s, message=%s
                WHERE id=%s
            """, (now, status, leads_fetched, enrolled, skipped, errors, message, run_id))


def get_run_logs(campaign_id=None, limit=50):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if campaign_id:
                cur.execute("""
                    SELECT * FROM run_log WHERE campaign_id=%s
                    ORDER BY started_at DESC LIMIT %s
                """, (campaign_id, limit))
            else:
                cur.execute("""
                    SELECT * FROM run_log
                    ORDER BY started_at DESC LIMIT %s
                """, (limit,))
            return [dict(r) for r in cur.fetchall()]


def get_today_enrolled(campaign_id):
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT enrolled FROM daily_counter
                WHERE campaign_id=%s AND date_str=%s
            """, (campaign_id, date_str))
            row = cur.fetchone()
    return row[0] if row else 0


def increment_today_enrolled(campaign_id, count):
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_counter (campaign_id, date_str, enrolled)
                VALUES (%s,%s,%s)
                ON CONFLICT (campaign_id, date_str)
                DO UPDATE SET enrolled = daily_counter.enrolled + %s
            """, (campaign_id, date_str, count, count))
