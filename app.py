"""
app.py — CRM Automation Builder (Streamlit)
Run with:  streamlit run app.py
"""
import os, sys, json, time
from datetime import datetime
from pathlib import Path

import streamlit as st
import pandas as pd

# ── Path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))

from core.db import (
    init_db, save_campaign, get_campaigns, get_campaign,
    delete_campaign, set_active, get_run_logs, get_today_enrolled
)
from core.scheduler import sync_jobs, run_now, get_scheduler

# ── Bootstrap ─────────────────────────────────────────────────────────────────
init_db()
get_scheduler()   # ensure scheduler is alive
sync_jobs()       # re-sync jobs on every page load

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CRM Automation Builder",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}

/* Dark industrial theme */
.stApp { background-color: #0d0f14; color: #c8cdd8; }

.block-container { padding-top: 2rem; max-width: 1400px; }

/* Header */
.app-header {
    background: linear-gradient(135deg, #1a1d26 0%, #0d0f14 100%);
    border: 1px solid #2a2f3d;
    border-radius: 8px;
    padding: 1.5rem 2rem;
    margin-bottom: 2rem;
    display: flex;
    align-items: center;
    gap: 1rem;
}
.app-header h1 {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.4rem;
    font-weight: 600;
    color: #e2e6f0;
    margin: 0;
}
.app-header .subtitle {
    font-size: 0.8rem;
    color: #5a6070;
    font-family: 'IBM Plex Mono', monospace;
    margin-top: 2px;
}

/* Status badge */
.badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 4px;
    font-size: 0.72rem;
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 600;
    letter-spacing: 0.05em;
}
.badge-active   { background: #0e3320; color: #4ade80; border: 1px solid #166534; }
.badge-inactive { background: #1a1d26; color: #5a6070; border: 1px solid #2a2f3d; }
.badge-running  { background: #1a2540; color: #60a5fa; border: 1px solid #1e3a5f; }
.badge-error    { background: #2a1010; color: #f87171; border: 1px solid #7f1d1d; }

/* Campaign card */
.campaign-card {
    background: #13161f;
    border: 1px solid #2a2f3d;
    border-radius: 8px;
    padding: 1.2rem 1.5rem;
    margin-bottom: 0.8rem;
    transition: border-color 0.2s;
}
.campaign-card:hover { border-color: #3d4557; }
.campaign-card.active { border-left: 3px solid #4ade80; }

/* Section headers */
.section-title {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.75rem;
    font-weight: 600;
    color: #5a6070;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 1rem;
    padding-bottom: 0.5rem;
    border-bottom: 1px solid #1e2230;
}

/* Metric cards */
.metric-row { display: flex; gap: 1rem; margin-bottom: 1.5rem; }
.metric-card {
    flex: 1;
    background: #13161f;
    border: 1px solid #2a2f3d;
    border-radius: 8px;
    padding: 1rem 1.2rem;
}
.metric-card .value {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 1.8rem;
    font-weight: 600;
    color: #e2e6f0;
    line-height: 1;
}
.metric-card .label {
    font-size: 0.75rem;
    color: #5a6070;
    margin-top: 4px;
    text-transform: uppercase;
    letter-spacing: 0.07em;
}

/* Log table tweaks */
.log-row-error { color: #f87171; }
.log-row-ok    { color: #4ade80; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: #0d0f14;
    border-right: 1px solid #1e2230;
}

/* Inputs */
.stTextInput input, .stNumberInput input, .stTextArea textarea {
    background: #1a1d26 !important;
    border: 1px solid #2a2f3d !important;
    color: #c8cdd8 !important;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.85rem;
}
.stSelectbox div[data-baseweb="select"] {
    background: #1a1d26 !important;
    border: 1px solid #2a2f3d !important;
}

/* Buttons */
.stButton > button {
    background: #1a1d26;
    border: 1px solid #2a2f3d;
    color: #c8cdd8;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem;
    border-radius: 5px;
    transition: all 0.2s;
}
.stButton > button:hover {
    background: #2a2f3d;
    border-color: #4a5568;
    color: #e2e6f0;
}

/* Accent button (primary) */
div[data-testid="stFormSubmitButton"] button {
    background: #0e3320 !important;
    border: 1px solid #166534 !important;
    color: #4ade80 !important;
    width: 100%;
    padding: 0.6rem;
    font-weight: 600;
}
div[data-testid="stFormSubmitButton"] button:hover {
    background: #14562e !important;
}

/* Dividers */
hr { border-color: #1e2230 !important; }

/* Tab styling */
.stTabs [data-baseweb="tab-list"] {
    background: #0d0f14;
    border-bottom: 1px solid #1e2230;
}
.stTabs [data-baseweb="tab"] {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.8rem;
    color: #5a6070;
}
.stTabs [aria-selected="true"] {
    color: #e2e6f0 !important;
    border-bottom: 2px solid #4ade80 !important;
}

/* Toggle */
.stToggle label { font-family: 'IBM Plex Mono', monospace; font-size: 0.85rem; }

/* Expander */
.streamlit-expanderHeader {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.82rem !important;
    background: #13161f !important;
    border: 1px solid #2a2f3d !important;
    border-radius: 6px !important;
    color: #c8cdd8 !important;
}
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR — navigation
# ═══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("""
    <div style="padding:1rem 0 1.5rem;">
        <div style="font-family:'IBM Plex Mono',monospace;font-size:1rem;
                    font-weight:600;color:#e2e6f0;">⚡ CRM Automation</div>
        <div style="font-size:0.72rem;color:#5a6070;margin-top:4px;">
            Cencorp → ActiveCampaign
        </div>
    </div>
    """, unsafe_allow_html=True)

    page = st.radio(
        "Navigation",
        ["🏠  Dashboard", "➕  New Campaign", "📋  Campaigns", "📜  Run Logs", "⚙️  Settings"],
        label_visibility="collapsed",
    )

    # Scheduler health
    sched = get_scheduler()
    status_color = "#4ade80" if sched.running else "#f87171"
    st.markdown(f"""
    <div style="margin-top:2rem;padding:0.8rem;background:#13161f;
                border:1px solid #2a2f3d;border-radius:6px;">
        <div style="font-size:0.7rem;color:#5a6070;font-family:'IBM Plex Mono',monospace;">
            SCHEDULER
        </div>
        <div style="margin-top:4px;display:flex;align-items:center;gap:6px;">
            <div style="width:7px;height:7px;border-radius:50%;
                        background:{status_color};"></div>
            <span style="font-family:'IBM Plex Mono',monospace;font-size:0.8rem;
                         color:{status_color};">
                {"RUNNING" if sched.running else "STOPPED"}
            </span>
        </div>
        <div style="font-size:0.68rem;color:#3a4050;margin-top:4px;
                    font-family:'IBM Plex Mono',monospace;">
            {len(sched.get_jobs())} active job(s)
        </div>
    </div>
    """, unsafe_allow_html=True)

    if st.button("🔄 Refresh", use_container_width=True):
        sync_jobs()
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════
if "Dashboard" in page:
    st.markdown("""
    <div class="app-header">
        <div>
            <div class="app-header h1" style="font-family:'IBM Plex Mono',monospace;
                font-size:1.3rem;font-weight:600;color:#e2e6f0;">
                ⚡ CRM Automation Builder
            </div>
            <div class="subtitle">Cencorp → ActiveCampaign · Drip Control · Background Scheduler</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    campaigns = get_campaigns()
    logs = get_run_logs(limit=100)

    active_count    = sum(1 for c in campaigns if c["active"])
    total_enrolled  = sum(l["enrolled"] or 0 for l in logs)
    today_str       = datetime.utcnow().strftime("%Y-%m-%d")
    today_enrolled  = sum(l["enrolled"] or 0 for l in logs
                          if l["started_at"].startswith(today_str))
    error_count     = sum(1 for l in logs if l["status"] == "error")

    # ── Metrics ───────────────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Campaigns", len(campaigns))
    with col2:
        st.metric("Active (Running)", active_count)
    with col3:
        st.metric("Enrolled Today", today_enrolled)
    with col4:
        st.metric("All-Time Enrolled", total_enrolled)

    st.divider()

    # ── Campaign quick-status ─────────────────────────────────────────────────
    st.markdown('<div class="section-title">Campaign Status</div>',
                unsafe_allow_html=True)

    if not campaigns:
        st.info("No campaigns yet. Create one from **➕ New Campaign**.")
    else:
        for c in campaigns:
            today_cnt = get_today_enrolled(c["id"])
            pct = int(today_cnt / c["drip_limit"] * 100) if c["drip_limit"] else 0
            badge = ("active" if c["active"] else "inactive")
            badge_label = "● ACTIVE" if c["active"] else "○ PAUSED"

            with st.container():
                col_a, col_b, col_c, col_d, col_e = st.columns([3, 1, 1, 1, 1])
                with col_a:
                    st.markdown(f"**{c['name']}**")
                    st.caption(f"Schedule: {c['start_hour']}:00 – {c['end_hour']}:00  {c['timezone']}")
                with col_b:
                    st.markdown(
                        f'<span class="badge badge-{badge}">{badge_label}</span>',
                        unsafe_allow_html=True
                    )
                with col_c:
                    st.metric("Drip Cap", c["drip_limit"])
                with col_d:
                    st.metric("Today", f"{today_cnt}/{c['drip_limit']}")
                with col_e:
                    if st.button("▶ Run Now", key=f"run_{c['id']}"):
                        run_now(c["id"])
                        st.toast(f"▶ {c['name']} triggered!", icon="⚡")
                st.progress(min(pct, 100))
                st.divider()

    # ── Recent log ────────────────────────────────────────────────────────────
    st.markdown('<div class="section-title">Recent Runs (last 10)</div>',
                unsafe_allow_html=True)
    recent = logs[:10]
    if recent:
        df = pd.DataFrame(recent)[[
            "campaign_name", "started_at", "status",
            "leads_fetched", "enrolled", "skipped", "errors"
        ]]
        df.columns = ["Campaign", "Started", "Status",
                      "Fetched", "Enrolled", "Skipped", "Errors"]
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.caption("No runs yet.")


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: NEW CAMPAIGN
# ═══════════════════════════════════════════════════════════════════════════════
elif "New Campaign" in page:
    st.subheader("➕ Create New Campaign")
    st.caption("Configure a re-engagement campaign. The query runs in the background within your schedule window.")

    # Pre-fill with the example query from the conversation
    DEFAULT_QUERY = {
        "query": {
            "$and": [
                {"asset": "63171df635754b46a4294287"},
                {"customerDetails.phoneNumber": {"$regex": "\\+971", "$options": "i"}},
                {"contactType": {"$in": ["Buyer"]}},
                {"officeName": {"$eq": "Abu Dhabi"}},
                {"minBedrooms": {"$gte": 3}},
                {"maxBedrooms": {"$lte": 9}},
                {"minPrice": {"$gte": 3500000}},
                {"maxPrice": {"$lte": 10000000}}
            ],
            "$or": [
                {"parentId": {"$exists": False}},
                {"parentId": {"$eq": None}}
            ]
        },
        "projection": {
            "_id": 1, "leadID": 1, "agent": 1,
            "customer": 1, "stage": 1, "status": 1,
            "createdAt": 1, "customerDetails": 1
        }
    }

    TIMEZONES = [
        "Asia/Dubai", "Asia/Riyadh", "Europe/London",
        "America/New_York", "America/Los_Angeles", "UTC"
    ]

    with st.form("new_campaign_form", clear_on_submit=False):
        st.markdown("#### Basic Info")
        col1, col2 = st.columns(2)
        with col1:
            name = st.text_input("Campaign Name *",
                                 placeholder="Abu Dhabi Buyers Re-engagement Q3")
        with col2:
            tz = st.selectbox("Timezone", TIMEZONES, index=0)

        st.markdown("#### ActiveCampaign")
        col3, col4 = st.columns(2)
        with col3:
            ac_tag_id = st.number_input("AC Tag ID *", min_value=1,
                                         value=1354, step=1)
        with col4:
            ac_auto_id = st.number_input("AC Automation ID *", min_value=1,
                                          value=337, step=1)

        st.markdown("#### Drip & Schedule")
        col5, col6, col7 = st.columns(3)
        with col5:
            drip_limit = st.number_input(
                "Daily Drip Limit",
                min_value=1, max_value=10000,
                value=100, step=10,
                help="Max contacts to enroll per day across all runs."
            )
        with col6:
            start_hour = st.slider("Start Hour (local)", 0, 23, 9,
                                   format="%d:00")
        with col7:
            end_hour = st.slider("End Hour (local)", 1, 23, 17,
                                 format="%d:00")

        if start_hour >= end_hour:
            st.warning("⚠ Start hour must be before end hour.")

        st.markdown("#### CRM Query (JSON)")
        st.caption("Paste your Cencorp filter query below. The `page` field is managed automatically.")
        query_str = st.text_area(
            "Query JSON",
            value=json.dumps(DEFAULT_QUERY, indent=2),
            height=350,
            help="The query sent as the `filter` param to Cencorp /properties/leads"
        )

        submitted = st.form_submit_button("💾 Save Campaign", use_container_width=True)

        if submitted:
            errors = []
            if not name.strip():
                errors.append("Campaign name is required.")
            if start_hour >= end_hour:
                errors.append("Start hour must be before end hour.")

            try:
                query_obj = json.loads(query_str)
            except json.JSONDecodeError as e:
                errors.append(f"Invalid JSON: {e}")
                query_obj = {}

            if errors:
                for e in errors:
                    st.error(e)
            else:
                try:
                    save_campaign(
                        name=name.strip(),
                        query_json=query_obj,
                        ac_tag_id=int(ac_tag_id),
                        ac_auto_id=int(ac_auto_id),
                        drip_limit=int(drip_limit),
                        start_hour=int(start_hour),
                        end_hour=int(end_hour),
                        timezone=tz,
                    )
                    st.success(f"✅ Campaign **{name}** saved! Go to **📋 Campaigns** to activate it.")
                except Exception as ex:
                    if "UNIQUE constraint" in str(ex):
                        st.error("A campaign with that name already exists.")
                    else:
                        st.error(f"Save failed: {ex}")


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: CAMPAIGNS (manage existing)
# ═══════════════════════════════════════════════════════════════════════════════
elif "Campaigns" in page:
    st.subheader("📋 Manage Campaigns")
    campaigns = get_campaigns()

    if not campaigns:
        st.info("No campaigns yet. Create one from **➕ New Campaign**.")
    else:
        for c in campaigns:
            is_active = bool(c["active"])
            border_color = "#166534" if is_active else "#2a2f3d"
            today_cnt = get_today_enrolled(c["id"])

            with st.expander(
                f"{'🟢' if is_active else '⚪'} {c['name']}  "
                f"— Drip: {today_cnt}/{c['drip_limit']} today",
                expanded=False
            ):
                col_info, col_actions = st.columns([3, 1])

                with col_info:
                    st.markdown(f"""
                    | Field | Value |
                    |-------|-------|
                    | **Status** | {'🟢 Active' if is_active else '⚪ Paused'} |
                    | **AC Tag ID** | `{c['ac_tag_id']}` |
                    | **AC Automation ID** | `{c['ac_auto_id']}` |
                    | **Daily Drip** | `{c['drip_limit']}` contacts/day |
                    | **Schedule** | `{c['start_hour']}:00 – {c['end_hour']}:00` ({c['timezone']}) |
                    | **Created** | `{c['created_at'][:19]}` |
                    | **Updated** | `{c['updated_at'][:19]}` |
                    """)

                with col_actions:
                    # Toggle active
                    new_state = st.toggle(
                        "Active",
                        value=is_active,
                        key=f"toggle_{c['id']}"
                    )
                    if new_state != is_active:
                        set_active(c["id"], new_state)
                        sync_jobs()
                        st.rerun()

                    if st.button("▶ Run Now", key=f"runnow_{c['id']}",
                                 use_container_width=True):
                        run_now(c["id"])
                        st.toast(f"▶ {c['name']} triggered!", icon="⚡")

                    st.markdown("---")
                    if st.button("✏️ Edit", key=f"edit_{c['id']}",
                                 use_container_width=True):
                        st.session_state["edit_campaign_id"] = c["id"]
                        st.rerun()

                    if st.button("🗑 Delete", key=f"del_{c['id']}",
                                 use_container_width=True,
                                 type="secondary"):
                        set_active(c["id"], False)
                        sync_jobs()
                        delete_campaign(c["id"])
                        st.toast(f"🗑 {c['name']} deleted.")
                        st.rerun()

                # Query preview
                with st.expander("🔍 View Query JSON"):
                    try:
                        q = json.loads(c["query_json"])
                        st.code(json.dumps(q, indent=2), language="json")
                    except Exception:
                        st.code(c["query_json"])

    # ── Inline editor ─────────────────────────────────────────────────────────
    if "edit_campaign_id" in st.session_state:
        eid = st.session_state["edit_campaign_id"]
        ec = get_campaign(eid)
        if ec:
            st.divider()
            st.subheader(f"✏️ Editing: {ec['name']}")
            TIMEZONES = [
                "Asia/Dubai", "Asia/Riyadh", "Europe/London",
                "America/New_York", "America/Los_Angeles", "UTC"
            ]
            tz_idx = TIMEZONES.index(ec["timezone"]) if ec["timezone"] in TIMEZONES else 0

            with st.form("edit_form"):
                name_e  = st.text_input("Name", value=ec["name"])
                col1, col2 = st.columns(2)
                with col1:
                    tag_e   = st.number_input("AC Tag ID", value=int(ec["ac_tag_id"]))
                    drip_e  = st.number_input("Daily Drip", value=int(ec["drip_limit"]))
                    sh_e    = st.slider("Start Hour", 0, 23, int(ec["start_hour"]))
                with col2:
                    auto_e  = st.number_input("AC Automation ID", value=int(ec["ac_auto_id"]))
                    tz_e    = st.selectbox("Timezone", TIMEZONES, index=tz_idx)
                    eh_e    = st.slider("End Hour", 1, 23, int(ec["end_hour"]))

                q_str = st.text_area(
                    "Query JSON",
                    value=json.dumps(json.loads(ec["query_json"]), indent=2),
                    height=300
                )

                col_save, col_cancel = st.columns(2)
                with col_save:
                    if st.form_submit_button("💾 Save Changes"):
                        try:
                            save_campaign(
                                name=name_e, query_json=json.loads(q_str),
                                ac_tag_id=int(tag_e), ac_auto_id=int(auto_e),
                                drip_limit=int(drip_e), start_hour=int(sh_e),
                                end_hour=int(eh_e), timezone=tz_e,
                                campaign_id=eid
                            )
                            del st.session_state["edit_campaign_id"]
                            sync_jobs()
                            st.success("Saved!")
                            st.rerun()
                        except Exception as ex:
                            st.error(str(ex))
                with col_cancel:
                    if st.form_submit_button("✕ Cancel"):
                        del st.session_state["edit_campaign_id"]
                        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: RUN LOGS
# ═══════════════════════════════════════════════════════════════════════════════
elif "Run Logs" in page:
    st.subheader("📜 Run Logs")

    campaigns = get_campaigns()
    camp_options = {"All Campaigns": None}
    camp_options.update({c["name"]: c["id"] for c in campaigns})

    col1, col2 = st.columns([2, 1])
    with col1:
        selected_camp = st.selectbox("Filter by Campaign", list(camp_options.keys()))
    with col2:
        limit = st.selectbox("Show last", [25, 50, 100, 200], index=1)

    cid_filter = camp_options[selected_camp]
    logs = get_run_logs(campaign_id=cid_filter, limit=limit)

    if not logs:
        st.info("No runs yet.")
    else:
        # Summary stats
        total_enrolled = sum(l["enrolled"] or 0 for l in logs)
        total_errors   = sum(l["errors"] or 0 for l in logs)
        success_runs   = sum(1 for l in logs if l["status"] == "completed")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Runs", len(logs))
        c2.metric("Successful", success_runs)
        c3.metric("Total Enrolled", total_enrolled)
        c4.metric("Total Errors", total_errors)

        st.divider()

        df = pd.DataFrame(logs)
        display_cols = ["campaign_name", "started_at", "finished_at",
                        "status", "leads_fetched", "enrolled",
                        "skipped", "errors", "message"]
        df = df[[c for c in display_cols if c in df.columns]]
        df.columns = [c.replace("_", " ").title() for c in df.columns]

        # Color-code status
        def color_status(val):
            colors = {
                "completed": "color: #4ade80",
                "error": "color: #f87171",
                "running": "color: #60a5fa",
            }
            return colors.get(val, "")

        st.dataframe(
            df.style.applymap(color_status, subset=["Status"])
            if "Status" in df.columns else df,
            use_container_width=True,
            hide_index=True,
        )


# ═══════════════════════════════════════════════════════════════════════════════
#  PAGE: SETTINGS
# ═══════════════════════════════════════════════════════════════════════════════
elif "Settings" in page:
    st.subheader("⚙️ Settings")

    st.markdown("#### API Credentials")
    st.caption("Set your Cencorp bearer token. The AC token is currently hardcoded — you can move it to env vars.")

    with st.form("settings_form"):
        cencorp_token = st.text_input(
            "Cencorp Bearer Token",
            value=os.environ.get("CENCORP_TOKEN", ""),
            type="password",
            help="This is stored in the session only — paste it on each restart, or set CENCORP_TOKEN env var."
        )
        if st.form_submit_button("Apply Token"):
            os.environ["CENCORP_TOKEN"] = cencorp_token
            st.success("Token set for this session. Set CENCORP_TOKEN env var to persist across restarts.")

    st.divider()
    st.markdown("#### Scheduler")
    sched = get_scheduler()

    jobs = sched.get_jobs()
    if jobs:
        job_data = [{"Job ID": j.id, "Next Run": str(j.next_run_time)} for j in jobs]
        st.dataframe(pd.DataFrame(job_data), use_container_width=True, hide_index=True)
    else:
        st.info("No scheduled jobs running. Activate a campaign to start scheduling.")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 Re-sync Jobs", use_container_width=True):
            sync_jobs()
            st.success("Jobs synced.")
            st.rerun()
    with col2:
        if st.button("⚠ Stop Scheduler", use_container_width=True):
            from core.scheduler import stop_scheduler
            stop_scheduler()
            st.warning("Scheduler stopped. Refresh to restart.")

    st.divider()
    st.markdown("#### Database")
    from core.db import DB_PATH
    st.code(f"DB location: {DB_PATH}", language="bash")
    st.caption("SQLite database stores all campaigns, run history, and daily counters.")

    if st.button("🗑 Clear All Run Logs (keep campaigns)", type="secondary"):
        from core.db import get_conn
        with get_conn() as conn:
            conn.execute("DELETE FROM run_log")
            conn.execute("DELETE FROM daily_counter")
        st.success("Logs cleared.")
        st.rerun()
