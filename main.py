#!/usr/bin/env python3
"""
Twenty CRM ↔ Excel two-way sync – CLI entry point.

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
        parts = "  ".join(f"{k}={v}" for k, v in counters.items())
        logger.info("  %s: %s", obj_key, parts)


# ── main ─────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Twenty CRM ↔ Excel two-way sync",
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="sync",
        choices=["sync", "pull", "push", "schedule", "health"],
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
    }
    commands[args.command]()


if __name__ == "__main__":
    main()
