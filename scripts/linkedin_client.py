"""
LinkedIn Member Data Portability API client.

Uses the Member Snapshot API to pull profile & connections data
for the authenticated LinkedIn member.

Docs: https://learn.microsoft.com/en-us/linkedin/dma/member-data-portability/shared/member-snapshot-api
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from .config import LINKEDIN_SNAPSHOT_DOMAINS, API_RATE_LIMIT_DELAY
from .linkedin_oauth import get_access_token

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.linkedin.com"
_API_VERSION = "202312"


class LinkedInAPIError(Exception):
    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"LinkedIn API {status_code}: {detail}")


class LinkedInClient:
    """Read-only client for the LinkedIn Member Data Portability APIs."""

    def __init__(self, access_token: str | None = None):
        self.access_token = access_token or get_access_token()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.access_token}",
                "Linkedin-Version": _API_VERSION,
                "Content-Type": "application/json",
            }
        )

    # ── low-level ────────────────────────────────────────────────────

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{_BASE_URL}{path}"
        logger.debug("GET %s  params=%s", url, params)
        resp = self.session.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            retry = float(resp.headers.get("Retry-After", 10))
            logger.warning("LinkedIn rate-limited – sleeping %.0fs", retry)
            time.sleep(retry)
            resp = self.session.get(url, params=params, timeout=30)
        if not resp.ok:
            raise LinkedInAPIError(resp.status_code, resp.text)
        return resp.json()

    # ── Member Snapshot API ──────────────────────────────────────────

    def get_snapshot(
        self, domain: str | None = None
    ) -> list[dict[str, Any]]:
        """
        Fetch member snapshot data, optionally filtered by *domain*
        (e.g. ``PROFILE``, ``CONNECTIONS``, ``POSITIONS``, etc.).

        Handles pagination automatically.
        """
        params: dict[str, Any] = {"q": "criteria"}
        if domain:
            params["domain"] = domain

        all_elements: list[dict] = []
        start = 0

        while True:
            params["start"] = start
            try:
                data = self._get("/rest/memberSnapshotData", params=params)
            except LinkedInAPIError as exc:
                # API signals "No data found" when we've exhausted pages
                if "No data found" in exc.detail:
                    break
                raise

            elements = data.get("elements", [])
            if not elements:
                break
            all_elements.extend(elements)

            # Check for next page
            paging = data.get("paging", {})
            links = paging.get("links", [])
            has_next = any(link.get("rel") == "next" for link in links)
            if not has_next:
                break
            start += 1
            time.sleep(API_RATE_LIMIT_DELAY)

        logger.info(
            "Fetched %d snapshot element(s) for domain=%s",
            len(all_elements),
            domain or "ALL",
        )
        return all_elements

    def get_profile(self) -> list[dict[str, Any]]:
        """Return flat profile records from the PROFILE snapshot domain."""
        elements = self.get_snapshot("PROFILE")
        records: list[dict] = []
        for el in elements:
            records.extend(el.get("snapshotData", []))
        return records

    def get_connections(self) -> list[dict[str, Any]]:
        """Return connection records from the CONNECTIONS snapshot domain."""
        elements = self.get_snapshot("CONNECTIONS")
        records: list[dict] = []
        for el in elements:
            records.extend(el.get("snapshotData", []))
        return records

    def get_all_domains(self) -> dict[str, list[dict]]:
        """
        Pull snapshot data for every domain listed in
        ``LINKEDIN_SNAPSHOT_DOMAINS`` and return ``{domain: [records]}``.
        """
        result: dict[str, list[dict]] = {}
        for domain in LINKEDIN_SNAPSHOT_DOMAINS:
            domain = domain.strip()
            if not domain:
                continue
            try:
                elements = self.get_snapshot(domain)
                records: list[dict] = []
                for el in elements:
                    records.extend(el.get("snapshotData", []))
                result[domain] = records
            except LinkedInAPIError:
                logger.exception("Failed to fetch domain %s", domain)
                result[domain] = []
        return result
