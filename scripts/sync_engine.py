"""
Two-way sync engine between Twenty CRM and a local Excel file.

Sync flow
─────────
1.  Fetch all records from CRM for each configured object.
2.  Read the matching Excel sheet.
3.  Compute a *diff* between the two data-sets using ``id`` as the key
    and ``updatedAt`` as the version vector.
4.  Apply the configured conflict-resolution strategy.
5.  Push changes in both directions (CRM → Excel, Excel → CRM).
6.  Persist the sync-state file so next run can detect deltas.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import (
    CONFLICT_STRATEGY,
    SYNC_OBJECTS,
    SYNC_STATE_PATH,
    EXCEL_FILE_PATH,
)
from .twenty_client import TwentyClient
from .excel_handler import (
    read_excel,
    write_excel,
    upsert_excel_rows,
    _flatten_value,
    _unflatten_value,
)

logger = logging.getLogger(__name__)

# ── sync state persistence ───────────────────────────────────────────


def _load_state() -> dict:
    p = Path(SYNC_STATE_PATH)
    if p.exists():
        return json.loads(p.read_text())
    return {}


def _save_state(state: dict) -> None:
    Path(SYNC_STATE_PATH).write_text(json.dumps(state, indent=2, default=str))


# ── timestamp helpers ────────────────────────────────────────────────


def _parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    try:
        s = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── diff logic ───────────────────────────────────────────────────────


def _build_lookup(records: list[dict], key: str = "id") -> dict[str, dict]:
    return {str(r[key]): r for r in records if r.get(key)}


def _fields_changed(
    crm_record: dict,
    excel_record: dict,
    fields: list[str],
) -> bool:
    """Return True if any tracked field differs between CRM and Excel."""
    for f in fields:
        crm_val = _flatten_value(crm_record.get(f))
        xl_val = excel_record.get(f)
        # Normalise both to str for comparison (handles None vs "")
        if _norm(crm_val) != _norm(xl_val):
            logger.debug(
                "Field '%s' differs: CRM=%r  Excel=%r", f, crm_val, xl_val
            )
            return True
    return False


def _norm(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    # Treat stringified empty composites as empty
    if s in ("None", "0", "0.0", "{}", "[]"):
        return ""
    return s


# ── core sync routine ────────────────────────────────────────────────


class SyncEngine:
    def __init__(self, client: TwentyClient | None = None):
        self.client = client or TwentyClient()
        self.state = _load_state()
        self.stats: dict[str, dict[str, int]] = {}

    # ── public entry point ───────────────────────────────────────────

    def sync_all(self) -> dict[str, dict[str, int]]:
        """Run a full two-way sync for every configured object."""
        for obj_key in SYNC_OBJECTS:
            self.stats[obj_key] = self._sync_object(obj_key)
        _save_state(self.state)
        return self.stats

    def pull(self) -> dict[str, dict[str, int]]:
        """CRM → Excel only (overwrite Excel with CRM data)."""
        for obj_key in SYNC_OBJECTS:
            crm_records = self.client.get_all_records(obj_key)
            write_excel(obj_key, crm_records)
            self._update_state_from_crm(obj_key, crm_records)
            self.stats[obj_key] = {"pulled": len(crm_records)}
        _save_state(self.state)
        return self.stats

    def push(self) -> dict[str, dict[str, int]]:
        """Excel → CRM only (push Excel changes to CRM)."""
        for obj_key in SYNC_OBJECTS:
            self.stats[obj_key] = self._push_object(obj_key)
        _save_state(self.state)
        return self.stats

    # ── internal ─────────────────────────────────────────────────────

    def _sync_object(self, obj_key: str) -> dict[str, int]:
        fields = SYNC_OBJECTS[obj_key]["fields"]
        counters = {
            "crm_to_excel": 0,
            "excel_to_crm_created": 0,
            "excel_to_crm_updated": 0,
            "conflicts": 0,
            "skipped": 0,
        }

        # 1. Fetch both sides ---------------------------------------------------
        crm_records = self.client.get_all_records(obj_key)
        excel_records = read_excel(obj_key)

        crm_by_id = _build_lookup(crm_records)
        xl_by_id = _build_lookup(excel_records)

        last_sync_ids: dict = self.state.get(obj_key, {})

        records_to_upsert_excel: list[dict] = []
        records_to_create_crm: list[dict] = []
        records_to_update_crm: list[dict] = []

        # 2. Walk CRM records ---------------------------------------------------
        for rid, crm_rec in crm_by_id.items():
            xl_rec = xl_by_id.get(rid)

            if xl_rec is None:
                # New in CRM → push to Excel
                records_to_upsert_excel.append(crm_rec)
                counters["crm_to_excel"] += 1
                continue

            if not _fields_changed(crm_rec, xl_rec, fields):
                counters["skipped"] += 1
                continue

            # Both sides exist & differ → conflict
            winner = self._resolve_conflict(crm_rec, xl_rec, obj_key)
            counters["conflicts"] += 1

            if winner == "crm":
                records_to_upsert_excel.append(crm_rec)
                counters["crm_to_excel"] += 1
            else:
                patch = self._excel_to_crm_payload(xl_rec, crm_rec, fields)
                patch["id"] = rid
                records_to_update_crm.append(patch)
                counters["excel_to_crm_updated"] += 1

        # 3. Walk Excel-only records (new in Excel) ----------------------------
        for rid, xl_rec in xl_by_id.items():
            if rid not in crm_by_id and rid not in ("", "None"):
                # Record exists in Excel with an ID but not in CRM – skip
                # (likely deleted on CRM side)
                counters["skipped"] += 1
                continue

        # Rows without an id → brand new records to create in CRM
        for xl_rec in excel_records:
            if not xl_rec.get("id"):
                payload = self._excel_to_crm_payload(xl_rec, None, fields)
                if any(v for v in payload.values()):
                    records_to_create_crm.append(payload)
                    counters["excel_to_crm_created"] += 1

        # 4. Apply changes ─────────────────────────────────────────────────────
        if records_to_upsert_excel:
            upsert_excel_rows(obj_key, records_to_upsert_excel)

        if records_to_create_crm:
            try:
                created = self.client.batch_create(obj_key, records_to_create_crm)
                # Write the newly-created records back to Excel (they now have ids)
                upsert_excel_rows(obj_key, created)
            except Exception:
                logger.exception("Batch create failed for %s", obj_key)

        if records_to_update_crm:
            try:
                self.client.batch_update(obj_key, records_to_update_crm)
            except Exception:
                logger.exception("Batch update failed for %s", obj_key)

        # 5. Refresh state ─────────────────────────────────────────────────────
        refreshed = self.client.get_all_records(obj_key)
        self._update_state_from_crm(obj_key, refreshed)
        # Overwrite Excel with the authoritative CRM data after sync
        write_excel(obj_key, refreshed)

        logger.info("Sync %s: %s", obj_key, counters)
        return counters

    def _push_object(self, obj_key: str) -> dict[str, int]:
        """Push Excel rows into CRM (create new, update existing)."""
        fields = SYNC_OBJECTS[obj_key]["fields"]
        counters = {"created": 0, "updated": 0}
        excel_records = read_excel(obj_key)

        crm_records = self.client.get_all_records(obj_key)
        crm_by_id = _build_lookup(crm_records)

        to_create: list[dict] = []
        to_update: list[dict] = []

        for xl_rec in excel_records:
            rid = xl_rec.get("id")
            if rid and str(rid) in crm_by_id:
                crm_rec = crm_by_id[str(rid)]
                if _fields_changed(crm_rec, xl_rec, fields):
                    patch = self._excel_to_crm_payload(xl_rec, crm_rec, fields)
                    patch["id"] = str(rid)
                    to_update.append(patch)
                    counters["updated"] += 1
            elif not rid:
                payload = self._excel_to_crm_payload(xl_rec, None, fields)
                if any(v for v in payload.values()):
                    to_create.append(payload)
                    counters["created"] += 1

        if to_create:
            created = self.client.batch_create(obj_key, to_create)
            upsert_excel_rows(obj_key, created)

        if to_update:
            self.client.batch_update(obj_key, to_update)

        return counters

    # ── conflict resolution ──────────────────────────────────────────

    def _resolve_conflict(
        self,
        crm_rec: dict,
        xl_rec: dict,
        obj_key: str,
    ) -> str:
        """Return ``'crm'`` or ``'excel'`` depending on strategy."""
        if CONFLICT_STRATEGY == "crm_wins":
            return "crm"
        if CONFLICT_STRATEGY == "excel_wins":
            return "excel"

        # newest_wins (default)
        crm_ts = _parse_ts(crm_rec.get("updatedAt"))
        xl_ts = _parse_ts(xl_rec.get("updatedAt"))
        if crm_ts and xl_ts:
            return "crm" if crm_ts >= xl_ts else "excel"
        return "crm"  # fallback

    # ── payload builders ─────────────────────────────────────────────

    @staticmethod
    def _excel_to_crm_payload(
        xl_rec: dict,
        crm_rec: dict | None,
        fields: list[str],
    ) -> dict:
        """
        Build a CRM-compatible dict from an Excel row.
        Uses the existing CRM record (if any) to rebuild composite values.
        """
        payload: dict[str, Any] = {}
        for f in fields:
            xl_val = xl_rec.get(f)
            if xl_val is None:
                continue
            existing = crm_rec.get(f) if crm_rec else None
            payload[f] = _unflatten_value(f, xl_val, existing)
        return payload

    # ── state management ─────────────────────────────────────────────

    def _update_state_from_crm(
        self, obj_key: str, records: list[dict]
    ) -> None:
        obj_state: dict[str, str] = {}
        for r in records:
            rid = r.get("id")
            updated = r.get("updatedAt")
            if rid:
                obj_state[str(rid)] = str(updated) if updated else _now_iso()
        self.state[obj_key] = obj_state
