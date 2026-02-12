"""
Twenty CRM REST API client.

Handles authentication, CRUD operations, and batch requests
for Companies and People objects via the /rest/ endpoint.
"""

from __future__ import annotations

import logging
import time
from typing import Any

import requests

from config import TWENTY_API_URL, TWENTY_API_KEY, API_RATE_LIMIT_DELAY, BATCH_SIZE

logger = logging.getLogger(__name__)


class TwentyAPIError(Exception):
    """Raised when the Twenty API returns an error."""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"HTTP {status_code}: {detail}")


class TwentyClient:
    """Thin wrapper around the Twenty CRM REST API."""

    def __init__(
        self,
        base_url: str = TWENTY_API_URL,
        api_key: str = TWENTY_API_KEY,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
        )

    # ── helpers ──────────────────────────────────────────────────────

    def _url(self, path: str) -> str:
        return f"{self.base_url}/rest/{path.lstrip('/')}"

    def _throttle(self) -> None:
        time.sleep(API_RATE_LIMIT_DELAY)

    def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> Any:
        url = self._url(path)
        logger.debug("%s %s", method, url)
        resp = self.session.request(method, url, **kwargs)
        if resp.status_code == 429:
            retry_after = float(resp.headers.get("Retry-After", 5))
            logger.debug("Rate-limited, retrying in %.1fs", retry_after)
            time.sleep(retry_after)
            resp = self.session.request(method, url, **kwargs)
        if not resp.ok:
            raise TwentyAPIError(resp.status_code, resp.text)
        if resp.status_code == 204:
            return None
        return resp.json()

    # ── generic CRUD ─────────────────────────────────────────────────

    def list_records(
        self,
        object_name: str,
        *,
        limit: int = 60,
        cursor: str | None = None,
        order_by: str | None = None,
        filter_: dict | None = None,
    ) -> dict:
        """
        GET /rest/{object_name}
        Returns {"data": {"[objectName]": [...]}, "pageInfo": {...}}
        """
        params: dict[str, Any] = {"limit": limit}
        if cursor:
            params["starting_after"] = cursor
        if order_by:
            params["order_by"] = order_by
        if filter_:
            params["filter"] = filter_

        result = self._request("GET", object_name, params=params)
        self._throttle()
        return result

    def get_all_records(self, object_name: str) -> list[dict]:
        """Page through ALL records for *object_name*."""
        all_records: list[dict] = []
        cursor: str | None = None
        while True:
            resp = self.list_records(object_name, limit=BATCH_SIZE, cursor=cursor)
            data = resp.get("data", {})
            # Twenty returns data under the pluralised object key
            records = data if isinstance(data, list) else data.get(object_name, data)
            if isinstance(records, dict):
                # Fallback: grab list from first key
                for v in records.values():
                    if isinstance(v, list):
                        records = v
                        break
            if not records:
                break
            all_records.extend(records)
            page_info = resp.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        logger.info("Fetched %d %s from CRM", len(all_records), object_name)
        return all_records

    def get_record(self, object_name: str, record_id: str) -> dict:
        """GET /rest/{object_name}/{id}"""
        result = self._request("GET", f"{object_name}/{record_id}")
        self._throttle()
        return result

    def create_record(self, object_name: str, data: dict) -> dict:
        """POST /rest/{object_name}"""
        result = self._request("POST", object_name, json=data)
        self._throttle()
        return result

    def update_record(
        self, object_name: str, record_id: str, data: dict
    ) -> dict:
        """PATCH /rest/{object_name}/{id}"""
        result = self._request(
            "PATCH", f"{object_name}/{record_id}", json=data
        )
        self._throttle()
        return result

    def delete_record(self, object_name: str, record_id: str) -> None:
        """DELETE /rest/{object_name}/{id}"""
        self._request("DELETE", f"{object_name}/{record_id}")
        self._throttle()

    # ── batch operations ─────────────────────────────────────────────

    def batch_create(
        self, object_name: str, records: list[dict]
    ) -> list[dict]:
        """POST /rest/batch/{object_name}  — up to 60 records at a time."""
        created: list[dict] = []
        for i in range(0, len(records), BATCH_SIZE):
            chunk = records[i : i + BATCH_SIZE]
            result = self._request(
                "POST", f"batch/{object_name}", json=chunk
            )
            created.extend(self._extract_records(result, object_name))
            self._throttle()
        return created

    def batch_update(
        self, object_name: str, records: list[dict]
    ) -> list[dict]:
        """PATCH /rest/batch/{object_name}  — each record must include 'id'."""
        updated: list[dict] = []
        for i in range(0, len(records), BATCH_SIZE):
            chunk = records[i : i + BATCH_SIZE]
            result = self._request(
                "PATCH", f"batch/{object_name}", json=chunk
            )
            if isinstance(result, list):
                updated.extend(result)
            elif isinstance(result, dict):
                updated.extend(self._extract_records(result, object_name))
            self._throttle()
        return updated

    @staticmethod
    def _extract_records(result: Any, object_name: str) -> list[dict]:
        """Robustly extract a list of record dicts from an API response."""
        if isinstance(result, list):
            return [r for r in result if isinstance(r, dict)]
        if not isinstance(result, dict):
            return []
        # Try {"data": {"people": [...]}}
        data = result.get("data", result)
        if isinstance(data, dict):
            for key in (object_name, object_name.rstrip("s")):
                if key in data and isinstance(data[key], list):
                    return [r for r in data[key] if isinstance(r, dict)]
            # Fallback: grab first list value
            for v in data.values():
                if isinstance(v, list):
                    return [r for r in v if isinstance(r, dict)]
            # Single record dict
            if "id" in data:
                return [data]
        if isinstance(data, list):
            return [r for r in data if isinstance(r, dict)]
        return []

    # ── health check ─────────────────────────────────────────────────

    def health(self) -> bool:
        """Returns True when the CRM server is reachable."""
        try:
            resp = self.session.get(
                f"{self.base_url}/healthz", timeout=5
            )
            return resp.ok
        except requests.RequestException:
            return False
