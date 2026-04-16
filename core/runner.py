"""
runner.py — Core automation logic
Fetches leads from Cencorp CRM page-by-page and enrolls them in ActiveCampaign.
Remembers the last processed page across runs so it never re-processes leads.
"""
import requests
import json
import time
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from .db import (
    get_campaign, start_run, finish_run,
    get_today_enrolled, increment_today_enrolled,
    set_last_page, reset_last_page
)

logger = logging.getLogger(__name__)

# ── API config ────────────────────────────────────────────────────────────────
CENCORP_BASE   = "https://api.cencorpcms.com"
CENCORP_LEADS  = f"{CENCORP_BASE}/properties/leads"

AC_BASE        = "https://bhomes.api-us1.com/api/3"
AC_TOKEN       = "10474935eaa36cb34609a1930d72444bb650c23fe8b83a0908b576f9ad655c11adf9a59a"

# ── Helpers ───────────────────────────────────────────────────────────────────

def cencorp_headers():
    import os
    token = os.environ.get("CENCORP_TOKEN", "")
    return {"Authorization": f"Bearer {token}"}


def ac_headers():
    return {
        "Api-Token": AC_TOKEN,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def is_within_schedule(start_hour: int, end_hour: int, tz_name: str) -> bool:
    """Return True if current local time is within [start_hour, end_hour)."""
    try:
        tz = ZoneInfo(tz_name)
        now = datetime.now(tz)
        return start_hour <= now.hour < end_hour
    except Exception:
        return True   # default: allow


def fetch_page(query: dict, page: int, page_size: int = 20) -> dict:
    """Fetch a single page of leads from Cencorp."""
    q = dict(query)
    q["page"] = page
    params = {"filter": json.dumps(q)}
    resp = requests.get(
        CENCORP_LEADS,
        headers=cencorp_headers(),
        params=params,
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def search_ac_contact(phone: str) -> list:
    """Search ActiveCampaign for a contact by phone. Returns list of contacts."""
    resp = requests.get(
        f"{AC_BASE}/contacts",
        headers=ac_headers(),
        params={"limit": 1, "phone": phone},
        timeout=30
    )
    if resp.status_code != 200:
        return []
    data = resp.json()
    return data.get("contacts", [])


def create_ac_contact(first_name: str, last_name: str,
                      phone: str, email: str) -> dict | None:
    """Create a contact in ActiveCampaign. Returns contact dict or None."""
    payload = {
        "contact": {
            "email": email,
            "firstName": first_name,
            "lastName": last_name,
            "phone": phone,
        }
    }
    resp = requests.post(
        f"{AC_BASE}/contacts",
        headers=ac_headers(),
        json=payload,
        timeout=30
    )
    if resp.status_code in (200, 201):
        return resp.json().get("contact")
    logger.warning("create_ac_contact failed %s: %s", phone, resp.text[:200])
    return None


def add_tag(contact_id: str | int, tag_id: int):
    payload = {"contactTag": {"contact": str(contact_id), "tag": str(tag_id)}}
    resp = requests.post(
        f"{AC_BASE}/contactTags",
        headers=ac_headers(),
        json=payload,
        timeout=30
    )
    return resp.status_code in (200, 201)


def add_to_automation(contact_id: str | int, automation_id: int):
    payload = {
        "contactAutomation": {
            "contact": str(contact_id),
            "automation": str(automation_id),
        }
    }
    resp = requests.post(
        f"{AC_BASE}/contactAutomations",
        headers=ac_headers(),
        json=payload,
        timeout=30
    )
    return resp.status_code in (200, 201)


def safe_email(first: str, last: str, phone: str) -> str:
    """Generate a placeholder email when none is available."""
    slug = (first + last).lower()
    slug = "".join(c for c in slug if c.isalnum()) or "unknown"
    digits = "".join(c for c in phone if c.isdigit())[-6:] or "000000"
    return f"a{slug}{digits}@placeholder.bhomes.com"


# ── Main runner ───────────────────────────────────────────────────────────────

def run_campaign(campaign_id: int, progress_callback=None):
    """
    Execute one campaign run:
      1. Check schedule window
      2. Check daily drip cap
      3. Resume from last saved page, paginate Cencorp, process each lead
      4. Save page progress after each page
      5. Reset to page 1 when all pages are exhausted
      6. Log results
    Returns a summary dict.
    """
    campaign = get_campaign(campaign_id)
    if not campaign:
        return {"error": "Campaign not found"}

    # ── Schedule check ────────────────────────────────────────────────────────
    if not is_within_schedule(campaign["start_hour"],
                               campaign["end_hour"],
                               campaign["timezone"]):
        msg = (f"Outside schedule window "
               f"{campaign['start_hour']}:00–{campaign['end_hour']}:00 "
               f"({campaign['timezone']})")
        logger.info("Campaign %s: %s", campaign["name"], msg)
        return {"skipped": True, "reason": msg}

    # ── Drip cap check ────────────────────────────────────────────────────────
    today_enrolled = get_today_enrolled(campaign_id)
    remaining = campaign["drip_limit"] - today_enrolled
    if remaining <= 0:
        msg = f"Daily drip cap of {campaign['drip_limit']} already reached today."
        logger.info("Campaign %s: %s", campaign["name"], msg)
        return {"skipped": True, "reason": msg}

    # ── Start logging ─────────────────────────────────────────────────────────
    run_id = start_run(campaign_id, campaign["name"])
    query = json.loads(campaign["query_json"])

    # ── Resume from last saved page ───────────────────────────────────────────
    page = campaign.get("last_page", 1)
    logger.info("Campaign %s: resuming from page %d", campaign["name"], page)

    stats = {"leads_fetched": 0, "enrolled": 0, "skipped": 0, "errors": 0}
    total_pages = None

    try:
        while True:
            # Re-check drip cap each page
            today_enrolled = get_today_enrolled(campaign_id)
            remaining = campaign["drip_limit"] - today_enrolled
            if remaining <= 0:
                logger.info("Drip cap hit mid-run. Stopping at page %d.", page)
                set_last_page(campaign_id, page)
                break

            if progress_callback:
                progress_callback(f"Fetching page {page}"
                                  + (f"/{total_pages}" if total_pages else "") + "…")

            try:
                data = fetch_page(query, page)
            except Exception as e:
                logger.error("Fetch page %d failed: %s", page, e)
                stats["errors"] += 1
                break

            leads = data.get("data", [])
            meta  = data.get("meta", {})
            total_pages = meta.get("totalPages", 1)
            stats["leads_fetched"] += len(leads)

            if not leads:
                # No more leads — reset to page 1 for next cycle
                logger.info("Campaign %s: all pages exhausted. Resetting to page 1.", campaign["name"])
                reset_last_page(campaign_id)
                break

            for lead in leads:
                if stats["enrolled"] >= remaining:
                    break

                details = lead.get("customerDetails", {})
                phone   = details.get("phoneNumber", "").strip()

                if not phone:
                    stats["skipped"] += 1
                    continue

                # ── Find or create AC contact ─────────────────────────────
                try:
                    existing = search_ac_contact(phone)
                    if existing:
                        contact_id = existing[0]["id"]
                    else:
                        first = details.get("firstName", "Unknown")
                        last  = details.get("lastName", "")
                        email = safe_email(first, last, phone)
                        contact = create_ac_contact(first, last, phone, email)
                        if not contact:
                            stats["errors"] += 1
                            continue
                        contact_id = contact["id"]

                    # ── Tag + enroll ──────────────────────────────────────
                    add_tag(contact_id, campaign["ac_tag_id"])
                    add_to_automation(contact_id, campaign["ac_auto_id"])
                    stats["enrolled"] += 1
                    increment_today_enrolled(campaign_id, 1)

                    # Polite rate limiting
                    time.sleep(0.3)

                except Exception as e:
                    logger.error("Lead processing error: %s", e)
                    stats["errors"] += 1
                    continue

            # ── Save page progress ────────────────────────────────────────
            if page >= total_pages:
                # Reached the last page — reset for next cycle
                logger.info("Campaign %s: reached last page %d. Resetting to page 1.", campaign["name"], page)
                reset_last_page(campaign_id)
                break
            elif stats["enrolled"] >= remaining:
                # Drip cap hit — save current page to resume tomorrow
                set_last_page(campaign_id, page + 1)
                break
            else:
                # More pages to go — save progress and continue
                set_last_page(campaign_id, page + 1)
                page += 1

        finish_run(run_id, status="completed", **stats,
                   message=f"Completed page {page}/{total_pages or '?'}")
        return {"status": "completed", **stats}

    except Exception as e:
        msg = f"Unexpected error: {e}"
        logger.exception(msg)
        finish_run(run_id, status="error", **stats, message=msg)
        return {"status": "error", "message": msg, **stats}
