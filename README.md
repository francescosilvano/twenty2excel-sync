# Twenty CRM ↔ Excel Two-Way Sync

A Python script that synchronizes **Companies** and **People** between your self-hosted [Twenty CRM](https://twenty.com) instance and a local `.xlsx` Excel workbook, supporting **two-way sync** with configurable conflict resolution.

---

## Features

| Feature | Details |
|---|---|
| **Pull** (CRM → Excel) | Downloads all records into styled Excel sheets |
| **Push** (Excel → CRM) | Creates new records / updates changed ones |
| **Two-way sync** | Detects changes on both sides, resolves conflicts |
| **Conflict strategies** | `newest_wins` · `crm_wins` · `excel_wins` |
| **Batch operations** | Up to 60 records per API call |
| **Rate-limit handling** | Automatic throttling + 429 back-off |
| **Scheduler** | Built-in interval-based scheduled sync |
| **Sync state** | Tracks last-known timestamps to detect deltas |

---

## Quick Start

### 1. Install dependencies

```bash
cd scripts/twenty_excel_sync
python -m venv .venv
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
python -m pip install -r requirements.txt
```

### 2. Configure

```bash
cp .env.example .env
```

- `TWENTY_API_URL` – your CRM base URL (e.g. `https://your-domain.example` for production, or `http://localhost:3000` for local development only)

- `TWENTY_API_URL` – your CRM base URL (e.g. `http://localhost:3000`)
- `TWENTY_API_KEY` – generate one in **Settings → APIs & Webhooks**

### 3. Run

```bash
# Check connectivity
python main.py health

# Pull all CRM data into Excel (good first run)
python main.py pull

# Push Excel edits back to CRM
python main.py push

# Full two-way sync
python main.py sync

# Run on a timer (default every 30 min)
python main.py schedule
```

Add `-v` for debug-level logging: `python main.py -v sync`

---

## How It Works

### Data Flow

```
┌──────────────┐       pull        ┌──────────────┐
│              │ ───────────────▷  │              │
│  Twenty CRM  │                   │  Excel .xlsx │
│              │ ◁───────────────  │              │
└──────────────┘       push        └──────────────┘
                  ◁──── sync ────▷
```

### Sync Logic

1. **Fetch** all records from the CRM via REST API.
2. **Read** the matching Excel sheet.
3. **Diff** by `id` — detect new, changed, or missing records on each side.
4. **Resolve conflicts** using the configured strategy:
   - `newest_wins` — the side with the newer `updatedAt` wins (default).
   - `crm_wins` — CRM always overwrites Excel.
   - `excel_wins` — Excel always overwrites CRM.
5. **Apply** changes: upsert into Excel and/or batch-create / batch-update
   in CRM.
6. **Persist** sync state (timestamps per record) to `.sync_state.json`.

### Adding New Records in Excel

Leave the **id** column empty for new rows. On the next `push` or `sync`,
the script will create the record in CRM and write the generated `id` back
into your spreadsheet.

---

## Project Structure

```
twenty_excel_sync/
├── main.py            # CLI entry point & scheduler
├── scripts/
│   ├── config.py          # Settings loader (.env + defaults)
│   ├── twenty_client.py   # Twenty CRM REST API client
│   ├── excel_handler.py   # Excel read / write / upsert
│   ├── sync_engine.py     # Two-way diff & conflict resolution
│   ├── linkedin_client.py # LinkedIn API client
│   ├── linkedin_oauth.py  # LinkedIn OAuth handler
│   └── linkedin_sync.py   # LinkedIn data sync
├── requirements.txt   # Python dependencies
├── .env.example       # Environment variable template
└── README.md          # ← you are here
```

---

## Configuration Reference

| Variable | Default | Description |
|---|---|---|
| `EXCEL_FILE_PATH` | `./twenty_crm_data.xlsx` | Path to the workbook (created automatically if missing) |
| `TWENTY_API_KEY` | *(required)* | Bearer token |
| `EXCEL_FILE_PATH` | `./twenty_crm_data.xlsx` | Path to the workbook |
| `CONFLICT_STRATEGY` | `newest_wins` | Conflict resolution mode |
| `API_RATE_LIMIT_DELAY` | `0.7` | Seconds between API calls |
| `BATCH_SIZE` | `60` | Records per batch call |
| `SYNC_INTERVAL_MINUTES` | `30` | Schedule interval |

---

## Extending to Other Objects

Edit `SYNC_OBJECTS` in `config.py` to add more Twenty objects (e.g.
`opportunities`, `notes`, `tasks`). Each entry needs:

```python
"opportunities": {
    "sheet_name": "Opportunities",
    "fields": ["name", "amount", "closeDate", "stage", ...],
},
```

The script will automatically create a new Excel sheet and sync it.

---

## LinkedIn Data Fetching

This feature allows you to fetch your LinkedIn profile data using LinkedIn's Member Data Portability API and integrate it with your CRM or Excel sheets.

### Prerequisites

1. **Create a LinkedIn App**: Go to [LinkedIn Developers](https://www.linkedin.com/developers/apps) and create a new app. Select the "Member Data Portability (Member) Default Company" page template.

2. **Request API Access**: In your app settings, request access to the "Member Data Portability API (Member)". This may take some time for approval.

3. **Configure Environment Variables**: Add the following to your `.env` file:
   - `LINKEDIN_CLIENT_ID`: Your LinkedIn app's client ID
   - `LINKEDIN_CLIENT_SECRET`: Your LinkedIn app's client secret
   - `LINKEDIN_REDIRECT_URI`: Callback URL (default: `http://localhost:8787/callback`)
   - `LINKEDIN_SCOPE`: API scope (default: `r_dma_portability_self_serve`)
   - `LINKEDIN_SNAPSHOT_DOMAINS`: Comma-separated list of data domains to fetch (e.g., `PROFILE,CONNECTIONS,POSITIONS,EDUCATION,SKILLS`)

### Steps

1. **Authenticate**: Run `python main.py linkedin-auth` to perform OAuth authentication and obtain an access token. This will open a browser for you to log in and authorize the app.

2. **Preview Data**: Optionally, run `python main.py linkedin-preview` to see what data is available without syncing.

3. **Sync Data**: Run `python main.py linkedin-sync` to fetch the LinkedIn data and integrate it with your CRM or Excel sheets. The data will be processed according to your configured snapshot domains.

---

## Scheduling with Cron (Linux) or Task Scheduler (Windows)

### Linux / WSL

```cron
*/30 * * * * cd /path/to/twenty_excel_sync && python main.py sync >> sync.log 2>&1
```

### Windows Task Scheduler

```powershell
python "C:\\path\\to\\twenty_excel_sync\\main.py" sync
python C:\path\to\twenty_excel_sync\main.py sync
```

Trigger: every 30 minutes. Alternatively, use the built-in scheduler:

```bash
python main.py schedule
```

## Acknowledgments

This project is built to integrate with [Twenty CRM](https://github.com/twentyhq/twenty), an open-source CRM platform developed by the Twenty team. I acknowledge and thank the Twenty community for their contributions to the open-source ecosystem.
