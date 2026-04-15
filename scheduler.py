"""
scheduler.py — APScheduler wrapper for background campaign execution.

We use a module-level singleton so Streamlit re-runs don't
spawn duplicate schedulers.
"""
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from .runner import run_campaign
from .db import get_campaigns

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def get_scheduler() -> BackgroundScheduler:
    global _scheduler
    if _scheduler is None or not _scheduler.running:
        _scheduler = BackgroundScheduler(timezone="UTC")
        _scheduler.start()
        logger.info("APScheduler started.")
    return _scheduler


def _campaign_job(campaign_id: int):
    """Job wrapper — runs a campaign and handles exceptions."""
    try:
        result = run_campaign(campaign_id)
        logger.info("Campaign %d result: %s", campaign_id, result)
    except Exception as e:
        logger.exception("Campaign %d scheduler error: %s", campaign_id, e)


def sync_jobs():
    """
    Sync APScheduler jobs with the current DB state.
    Called on app startup and whenever the user toggles a campaign on/off.
    """
    sched = get_scheduler()
    campaigns = get_campaigns()
    active_ids = {c["id"] for c in campaigns if c["active"]}

    # Build current job map
    existing = {int(j.id): j for j in sched.get_jobs()
                if j.id.startswith("campaign_")}
    existing_ids = {int(jid.replace("campaign_", ""))
                    for jid in existing}

    # Remove deactivated jobs
    for cid in existing_ids - active_ids:
        sched.remove_job(f"campaign_{cid}")
        logger.info("Removed job for campaign %d", cid)

    # Add missing active jobs (every 5 minutes)
    for cid in active_ids - existing_ids:
        sched.add_job(
            _campaign_job,
            trigger=IntervalTrigger(minutes=5),
            id=f"campaign_{cid}",
            args=[cid],
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        logger.info("Added job for campaign %d (every 5 min)", cid)


def run_now(campaign_id: int):
    """Manually trigger a campaign immediately (non-blocking)."""
    sched = get_scheduler()
    sched.add_job(
        _campaign_job,
        id=f"manual_{campaign_id}_{__import__('time').time_ns()}",
        args=[campaign_id],
        max_instances=1,
    )


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        _scheduler = None
