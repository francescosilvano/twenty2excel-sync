"""
Configuration for Twenty CRM ↔ Excel sync.
Loads settings from environment variables or .env file.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
_env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(_env_path)


# ── Twenty CRM connection ────────────────────────────────────────────
TWENTY_API_URL = os.getenv("TWENTY_API_URL", "http://localhost:3000")
TWENTY_API_KEY = os.getenv("TWENTY_API_KEY", "")

# ── Excel file path ──────────────────────────────────────────────────
EXCEL_FILE_PATH = os.getenv(
    "EXCEL_FILE_PATH",
    str(Path(__file__).resolve().parent / "twenty_crm_data.xlsx"),
)

# ── Sync state file (stores last-sync timestamps per record) ─────────
SYNC_STATE_PATH = os.getenv(
    "SYNC_STATE_PATH",
    str(Path(__file__).resolve().parent / ".sync_state.json"),
)

# ── Objects to sync ──────────────────────────────────────────────────
# Each entry maps an object name to the fields we care about.
# "id" and "updatedAt" are always included automatically.
SYNC_OBJECTS = {
    "companies": {
        "sheet_name": "Companies",
        "fields": [
            "name",
            "domainName",
            "address",
            "employees",
            "linkedinLink",
            "annualRecurringRevenue",
            "idealCustomerProfile",
            "position",
        ],
    },
    "people": {
        "sheet_name": "People",
        "fields": [
            "name",
            "emails",
            "phones",
            "city",
            "jobTitle",
            "linkedinLink",
            "position",
        ],
    },
}

# ── Rate-limit settings ─────────────────────────────────────────────
API_RATE_LIMIT_DELAY = float(os.getenv("API_RATE_LIMIT_DELAY", "0.7"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "60"))

# ── Conflict resolution strategy ─────────────────────────────────────
# Options: "crm_wins", "excel_wins", "newest_wins"
CONFLICT_STRATEGY = os.getenv("CONFLICT_STRATEGY", "newest_wins")

# ── Scheduler ────────────────────────────────────────────────────────
SYNC_INTERVAL_MINUTES = int(os.getenv("SYNC_INTERVAL_MINUTES", "30"))

# ── LinkedIn OAuth / API ─────────────────────────────────────────────
LINKEDIN_CLIENT_ID = os.getenv("LINKEDIN_CLIENT_ID", "")
LINKEDIN_CLIENT_SECRET = os.getenv("LINKEDIN_CLIENT_SECRET", "")
LINKEDIN_REDIRECT_URI = os.getenv(
    "LINKEDIN_REDIRECT_URI", "http://localhost:8787/callback"
)
LINKEDIN_SCOPE = os.getenv(
    "LINKEDIN_SCOPE", "r_dma_portability_self_serve"
)
LINKEDIN_TOKEN_PATH = os.getenv(
    "LINKEDIN_TOKEN_PATH",
    str(Path(__file__).resolve().parent / ".linkedin_token.json"),
)
# Access token – paste from LinkedIn's OAuth Token Generator Tool
LINKEDIN_ACCESS_TOKEN = os.getenv("LINKEDIN_ACCESS_TOKEN", "")
# Snapshot domains to pull (comma-separated).
# Common: PROFILE, CONNECTIONS, POSITIONS, EDUCATION, SKILLS
LINKEDIN_SNAPSHOT_DOMAINS = os.getenv(
    "LINKEDIN_SNAPSHOT_DOMAINS", "PROFILE,CONNECTIONS"
).split(",")
