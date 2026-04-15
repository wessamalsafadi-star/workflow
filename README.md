# CRM Automation Builder

A Streamlit app that replaces your n8n workflow.  
**Cencorp CRM → ActiveCampaign** with drip control, schedule windows, and background execution.

---

## Quick Start

### 1. Install dependencies
```bash
cd crm_automation
pip install -r requirements.txt
```

### 2. Set your Cencorp token
```bash
export CENCORP_TOKEN="your_bearer_token_here"
```
Or set it in the **Settings** page inside the app each session.

### 3. Run
```bash
streamlit run app.py
```
Opens at http://localhost:8501

---

## How it works

### Campaign lifecycle
1. **Create** a campaign — give it a name, paste your Cencorp JSON query, set AC tag/automation IDs, drip limit, and schedule window.
2. **Activate** the campaign toggle — this registers a background job that runs every 5 minutes.
3. The job checks:
   - Is the current time within the schedule window? (e.g. 9:00–17:00 Dubai time)
   - Has today's drip cap been hit?
   - If yes to both → starts paginating Cencorp and enrolling contacts.
4. For each lead: search AC by phone → create if not found → add tag → add to automation.
5. All runs are logged to SQLite with full stats.

### Schedule window
Each campaign has its own `start_hour` / `end_hour` in a configurable timezone. The 5-minute cron fires continuously, but no work happens outside the window.

### Drip control
`drip_limit` is the **daily max enrollments** per campaign. It resets at midnight UTC. The counter is stored in SQLite.

### Pagination
The runner fetches Cencorp page by page until either:
- All pages are exhausted, or
- The daily drip cap is hit

The page number is managed automatically — no more manual `"page": 8` in your query.

### Persistence
All data lives in `~/crm_campaigns.db` (SQLite). This survives app restarts. Change the path with the `CRM_DB_PATH` env var.

---

## File structure

```
crm_automation/
├── app.py              # Streamlit UI
├── requirements.txt
└── core/
    ├── __init__.py
    ├── db.py           # SQLite: campaigns, run_log, daily_counter
    ├── runner.py       # Cencorp pagination + AC enrollment logic
    └── scheduler.py    # APScheduler background runner
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `CENCORP_TOKEN` | _(required)_ | Bearer token for Cencorp API |
| `CRM_DB_PATH` | `~/crm_campaigns.db` | SQLite database path |

---

## Duplicating a workflow

To run the same query for a different tag/automation/drip config:
1. Go to **➕ New Campaign**
2. Give it a different name
3. Paste the same query JSON
4. Set different AC Tag ID, Automation ID, or drip limit
5. Save and activate

Each campaign runs independently with its own drip counter and schedule.

---

## Running as a persistent background service (optional)

To keep the app alive 24/7 without a browser window, run it as a service:

```bash
# Using nohup
nohup streamlit run app.py --server.port 8501 > crm.log 2>&1 &

# Using screen
screen -S crm
streamlit run app.py
# Ctrl+A D to detach

# Using systemd (create /etc/systemd/system/crm-automation.service)
[Unit]
Description=CRM Automation Builder
After=network.target

[Service]
User=youruser
WorkingDirectory=/path/to/crm_automation
Environment="CENCORP_TOKEN=your_token_here"
ExecStart=/usr/bin/streamlit run app.py --server.port 8501
Restart=always

[Install]
WantedBy=multi-user.target
```
