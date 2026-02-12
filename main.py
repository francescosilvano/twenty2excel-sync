#!/usr/bin/env python3
"""
Twenty CRM ↔ Excel two-way sync + LinkedIn integration – CLI entry point.

Usage
─────
  # First run – pull everything from CRM into a fresh Excel file
  python main.py pull

  # Push local Excel changes into CRM
  python main.py push

  # Full two-way sync (default)
  python main.py sync

  # Run on a schedule (default: every 30 min, configurable via SYNC_INTERVAL_MINUTES)
  python main.py schedule

  # Health-check your CRM connection
  python main.py health

  # LinkedIn integration
  python main.py linkedin-auth       # Authenticate with LinkedIn (opens browser)
  python main.py linkedin-sync       # Pull connections into CRM + Excel
  python main.py linkedin-preview    # Preview data without writing (dry run)
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
import time
from datetime import datetime

from config import SYNC_INTERVAL_MINUTES
from twenty_client import TwentyClient
from sync_engine import SyncEngine

# ── logging setup ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("twenty-sync")

# ── graceful shutdown ────────────────────────────────────────────────
_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    logger.info("Received signal %s – shutting down after current cycle", signum)
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── CLI commands ─────────────────────────────────────────────────────


def cmd_health(client: TwentyClient) -> None:
    if client.health():
        logger.info("Twenty CRM is reachable ✓")
    else:
        logger.error("Cannot reach Twenty CRM – check TWENTY_API_URL and network")
        sys.exit(1)


def cmd_pull(engine: SyncEngine) -> None:
    logger.info("── PULL: CRM → Excel ──")
    stats = engine.pull()
    _print_stats(stats)


def cmd_push(engine: SyncEngine) -> None:
    logger.info("── PUSH: Excel → CRM ──")
    stats = engine.push()
    _print_stats(stats)


def cmd_sync(engine: SyncEngine) -> None:
    logger.info("── TWO-WAY SYNC ──")
    stats = engine.sync_all()
    _print_stats(stats)


def cmd_schedule(engine: SyncEngine) -> None:
    interval = SYNC_INTERVAL_MINUTES * 60
    logger.info(
        "Scheduled sync every %d minutes. Press Ctrl+C to stop.", SYNC_INTERVAL_MINUTES
    )
    while not _shutdown:
        try:
            logger.info("── Scheduled sync at %s ──", datetime.now().isoformat())
            stats = engine.sync_all()
            _print_stats(stats)
        except Exception:
            logger.exception("Sync cycle failed – will retry next interval")
        if _shutdown:
            break
        logger.info("Next sync in %d minutes…", SYNC_INTERVAL_MINUTES)
        # Sleep in small increments so we can catch signals quickly
        for _ in range(interval):
            if _shutdown:
                break
            time.sleep(1)
    logger.info("Scheduler stopped.")


def _print_stats(stats: dict) -> None:
    for obj_key, counters in stats.items():
        if isinstance(counters, dict):
            parts = "  ".join(f"{k}={v}" for k, v in counters.items())
            logger.info("  %s: %s", obj_key, parts)
        else:
            logger.info("  %s: %s", obj_key, counters)


# ── LinkedIn commands ────────────────────────────────────────────────


def cmd_linkedin_auth() -> None:
    from config import LINKEDIN_ACCESS_TOKEN
    from linkedin_oauth import authenticate, save_manual_token, get_access_token
    logger.info("── LINKEDIN AUTH ──")

    # If token is already in .env, just validate and confirm
    if LINKEDIN_ACCESS_TOKEN:
        logger.info("Found LINKEDIN_ACCESS_TOKEN in .env ✓")
        logger.info("Token: %s…%s", LINKEDIN_ACCESS_TOKEN[:8], LINKEDIN_ACCESS_TOKEN[-4:])
        return

    print("\nNo LINKEDIN_ACCESS_TOKEN found in .env.")
    print("Options:")
    print("  1) Paste it into your .env file as LINKEDIN_ACCESS_TOKEN=<token>")
    print("  2) Run the full browser-based OAuth flow")
    choice = input("\nEnter 1 or 2: ").strip()

    if choice == "1":
        print("Add your token to .env and re-run this command.")
    else:
        authenticate()


def cmd_linkedin_sync(client: TwentyClient) -> None:
    from linkedin_sync import LinkedInSync
    logger.info("── LINKEDIN → CRM SYNC ──")

    print("\nWhat do you want to sync?")
    print("  1) People + Companies")
    print("  2) People only")
    print("  3) Companies only")
    choice = input("\nEnter 1, 2 or 3 [1]: ").strip() or "1"

    scope = {"1": "both", "2": "people", "3": "companies"}.get(choice, "both")
    logger.info("Scope: %s", scope)

    syncer = LinkedInSync(twenty=client)
    syncer.sync(dry_run=False, scope=scope)


def cmd_linkedin_preview() -> None:
    from linkedin_client import LinkedInClient
    logger.info("── LINKEDIN PREVIEW (dry run) ──")
    li = LinkedInClient()
    data = li.get_all_domains()
    for domain, records in data.items():
        logger.info("  %s: %d records", domain, len(records))
        for rec in records[:3]:
            logger.info("    %s", rec)


# ── main ─────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Twenty CRM ↔ Excel two-way sync",
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="sync",
        choices=[
            "sync", "pull", "push", "schedule", "health",
            "linkedin-auth", "linkedin-sync", "linkedin-preview",
        ],
        help="Action to perform (default: sync)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    client = TwentyClient()
    engine = SyncEngine(client)

    commands = {
        "health": lambda: cmd_health(client),
        "pull": lambda: cmd_pull(engine),
        "push": lambda: cmd_push(engine),
        "sync": lambda: cmd_sync(engine),
        "schedule": lambda: cmd_schedule(engine),
        "linkedin-auth": lambda: cmd_linkedin_auth(),
        "linkedin-sync": lambda: cmd_linkedin_sync(client),
        "linkedin-preview": lambda: cmd_linkedin_preview(),
    }
    commands[args.command]()


if __name__ == "__main__":
    main()
