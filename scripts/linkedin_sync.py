"""
LinkedIn → Twenty CRM sync module.

Maps LinkedIn Member Snapshot data (profile & connections) into
Twenty CRM People and Companies records, then pushes them via the
existing Twenty API client.
"""

from __future__ import annotations

import logging
import sys
import threading
import time
from contextlib import contextmanager
from typing import Any, Generator

from .twenty_client import TwentyClient
from .linkedin_client import LinkedInClient
from .excel_handler import upsert_excel_rows

logger = logging.getLogger(__name__)


@contextmanager
def spinner(message: str, total: int = 0) -> Generator[Any, None, None]:
    """Show an animated spinner with optional n/N progress counter."""
    stop = threading.Event()
    frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    progress = {"current": 0}

    def _spin() -> None:
        i = 0
        while not stop.is_set():
            if total > 0:
                pct = f" {progress['current']}/{total}"
            else:
                pct = ""
            sys.stdout.write(f"\r  {frames[i % len(frames)]} {message}{pct}  ")
            sys.stdout.flush()
            i += 1
            stop.wait(0.08)
        if total > 0:
            sys.stdout.write(f"\r  ✓ {message} {progress['current']}/{total}  \n")
        else:
            sys.stdout.write(f"\r  ✓ {message}  \n")
        sys.stdout.flush()

    t = threading.Thread(target=_spin, daemon=True)
    t.start()
    try:
        yield progress
    finally:
        stop.set()
        t.join()


# ── Field mapping: LinkedIn snapshot → Twenty CRM ────────────────────

def _map_connection_to_person(conn: dict[str, Any]) -> dict[str, Any]:
    """
    Convert a LinkedIn CONNECTIONS snapshot record into a Twenty
    People-compatible payload.

    Typical LinkedIn connection keys:
        First Name, Last Name, Email Address, Company, Position,
        Connected On, URL
    """
    first = conn.get("First Name", "") or ""
    last = conn.get("Last Name", "") or ""
    email = conn.get("Email Address", "") or ""
    company = conn.get("Company", "") or ""
    position = conn.get("Position", "") or ""
    url = conn.get("URL", "") or ""

    person: dict[str, Any] = {
        "name": {"firstName": first, "lastName": last},
    }
    if email:
        person["emails"] = {"primaryEmail": email}
    if position:
        person["jobTitle"] = position
    if url:
        person["linkedinLink"] = {"primaryLinkUrl": url}

    # Attach company name as metadata (used later for linking)
    person["_linkedin_company"] = company
    person["_linkedin_connected_on"] = conn.get("Connected On", "")

    return person


def _map_profile_to_person(profile: dict[str, Any]) -> dict[str, Any]:
    """
    Convert a LinkedIn PROFILE snapshot record into a Twenty
    People-compatible payload.
    """
    first = profile.get("First Name", "") or ""
    last = profile.get("Last Name", "") or ""
    headline = profile.get("Headline", "") or ""

    return {
        "name": {"firstName": first, "lastName": last},
        "jobTitle": headline,
    }


def _extract_company_names(connections: list[dict]) -> set[str]:
    """Pull unique company names from connection records."""
    names: set[str] = set()
    for conn in connections:
        co = conn.get("Company", "") or conn.get("_linkedin_company", "")
        co = co.strip()
        if co:
            names.add(co)
    return names


# ── Sync orchestrator ────────────────────────────────────────────────


class LinkedInSync:
    """Syncs LinkedIn data into Twenty CRM People / Companies."""

    def __init__(
        self,
        twenty: TwentyClient | None = None,
        linkedin: LinkedInClient | None = None,
    ):
        self.twenty = twenty or TwentyClient()
        self.linkedin = linkedin or LinkedInClient()

    def sync(
        self,
        *,
        dry_run: bool = False,
        scope: str = "both",
    ) -> dict[str, int]:
        """
        Pull LinkedIn connections & profile, upsert into Twenty CRM.

        *scope* controls what gets synced:
            "people"    – only People records
            "companies" – only Company records
            "both"      – People + Companies (default)

        Returns a dict of counters.
        """
        sync_people = scope in ("people", "both")
        sync_companies = scope in ("companies", "both")

        counters = {
            "connections_fetched": 0,
            "people_created": 0,
            "people_updated": 0,
            "people_skipped": 0,
            "companies_created": 0,
            "companies_skipped": 0,
        }

        # 1. Fetch LinkedIn data ──────────────────────────────────────
        with spinner("Fetching LinkedIn connections"):
            raw_connections = self.linkedin.get_connections()
        counters["connections_fetched"] = len(raw_connections)
        logger.info("%d connections fetched", len(raw_connections))

        # 2. Map to CRM payloads ──────────────────────────────────────
        mapped_people = [_map_connection_to_person(c) for c in raw_connections]

        # 3. Ensure companies exist ───────────────────────────────────
        company_map: dict[str, str] = {}
        if sync_companies:
            company_names = _extract_company_names(mapped_people)
            company_map = self._ensure_companies(company_names, dry_run, counters)
        else:
            logger.info("Skipping companies (scope=%s)", scope)

        # 4. Upsert people ────────────────────────────────────────────
        if sync_people:
            self._upsert_people(mapped_people, company_map, dry_run, counters)
        else:
            logger.info("Skipping people (scope=%s)", scope)

        logger.info("── Sync complete ──")
        return counters

    # ── Companies ────────────────────────────────────────────────────

    def _ensure_companies(
        self,
        names: set[str],
        dry_run: bool,
        counters: dict[str, int],
    ) -> dict[str, str]:
        """
        For each company name, check if it exists in CRM by name.
        Create it if not.  Returns ``{name: crm_id}``.
        """
        existing = self.twenty.get_all_records("companies")
        name_to_id: dict[str, str] = {}

        for rec in existing:
            n = rec.get("name")
            if isinstance(n, dict):
                n = n.get("firstName", "") or str(n)
            if n:
                name_to_id[str(n).strip().lower()] = rec["id"]

            domain = rec.get("domainName", "")
            if isinstance(domain, dict):
                domain = domain.get("primaryLinkUrl", "") or ""
            if domain:
                name_to_id[f"_domain_{str(domain).strip().lower()}"] = rec["id"]

        failed: list[str] = []
        name_list = sorted(names)
        total = len(name_list)

        with spinner("Syncing companies", total) as prog:
            for i, name in enumerate(name_list, 1):
                prog["current"] = i
                key = name.strip().lower()
                if key in name_to_id:
                    counters["companies_skipped"] += 1
                    continue
                if dry_run:
                    counters["companies_created"] += 1
                    continue
                try:
                    created = self.twenty.create_record(
                        "companies", {"name": name}
                    )
                    cid = ""
                    data = created.get("data", created)
                    if isinstance(data, dict):
                        cid = data.get("id", "")
                        if not cid:
                            for v in data.values():
                                if isinstance(v, dict) and v.get("id"):
                                    cid = v["id"]
                                    break
                    if not cid:
                        cid = created.get("id", "")

                    name_to_id[key] = cid
                    counters["companies_created"] += 1
                except Exception as exc:
                    failed.append(name)
                    logger.debug("Failed to create company '%s': %s", name, exc)

        if dry_run:
            logger.info("[DRY RUN] Would create %d companies", counters["companies_created"])
        else:
            logger.info(
                "Companies: %d created, %d already existed",
                counters["companies_created"],
                counters["companies_skipped"],
            )
        if failed:
            logger.warning("%d companies failed: %s", len(failed), ", ".join(failed))

        return name_to_id

    # ── People ───────────────────────────────────────────────────────

    def _upsert_people(
        self,
        mapped: list[dict],
        company_map: dict[str, str],
        dry_run: bool,
        counters: dict[str, int],
    ) -> None:
        """Match by LinkedIn URL or name; create/update as needed."""
        existing_people = self.twenty.get_all_records("people")

        # Build lookup by linkedin URL
        url_to_rec: dict[str, dict] = {}
        for p in existing_people:
            link = p.get("linkedinLink")
            if isinstance(link, dict):
                url = link.get("primaryLinkUrl", "")
            else:
                url = str(link) if link else ""
            if url:
                url_to_rec[url.rstrip("/")] = p

        # Build lookup by full name (fallback)
        name_to_rec: dict[str, dict] = {}
        for p in existing_people:
            raw_name = p.get("name")
            if isinstance(raw_name, dict):
                full = f"{raw_name.get('firstName', '')} {raw_name.get('lastName', '')}".strip()
            else:
                full = str(raw_name).strip() if raw_name else ""
            if full:
                name_to_rec[full.lower()] = p

        to_create: list[dict] = []
        to_update: list[dict] = []
        records_for_excel: list[dict] = []
        total = len(mapped)

        with spinner("Processing people", total) as prog:
            for i, person in enumerate(mapped, 1):
                prog["current"] = i
                li_url = ""
                link_val = person.get("linkedinLink")
                if isinstance(link_val, dict):
                    li_url = link_val.get("primaryLinkUrl", "").rstrip("/")
                elif link_val:
                    li_url = str(link_val).rstrip("/")

                name_dict = person.get("name", {})
                full_name = f"{name_dict.get('firstName', '')} {name_dict.get('lastName', '')}".strip()

                # Try to find existing CRM record
                existing = None
                if li_url and li_url in url_to_rec:
                    existing = url_to_rec[li_url]
                elif full_name and full_name.lower() in name_to_rec:
                    existing = name_to_rec[full_name.lower()]

                # Attach company reference if available
                company_name = person.pop("_linkedin_company", "")
                person.pop("_linkedin_connected_on", None)
                if company_name:
                    cid = company_map.get(company_name.strip().lower())
                    if cid:
                        person["company"] = {"_ref": cid}

                if existing:
                    # Only update if there's new data to push
                    patch = {k: v for k, v in person.items() if v and k != "company"}
                    if patch:
                        patch["id"] = existing["id"]
                        to_update.append(patch)
                        counters["people_updated"] += 1
                    else:
                        counters["people_skipped"] += 1
                else:
                    to_create.append(person)
                    counters["people_created"] += 1

        if dry_run:
            logger.info(
                "[DRY RUN] Would create %d / update %d people",
                len(to_create),
                len(to_update),
            )
            return

        # Batch create
        if to_create:
            try:
                created = self.twenty.batch_create("people", to_create)
                records_for_excel.extend(created)
            except Exception as exc:
                logger.warning("Batch create people failed: %s", exc)

        # Batch update
        if to_update:
            try:
                self.twenty.batch_update("people", to_update)
            except Exception as exc:
                logger.warning("Batch update people failed: %s", exc)

        logger.info(
            "People: %d created, %d updated, %d skipped",
            counters["people_created"],
            counters["people_updated"],
            counters["people_skipped"],
        )

        # Sync back to Excel
        if records_for_excel:
            try:
                upsert_excel_rows("people", records_for_excel)
            except Exception:
                logger.exception("Failed to write LinkedIn records to Excel")
