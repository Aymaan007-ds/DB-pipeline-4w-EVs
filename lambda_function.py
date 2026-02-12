"""Deployable AWS Lambda for multi-agent EV spec curation + Postgres persistence.

Why this module exists:
- Keep a guard-railed, multi-agent enrichment flow (primary -> gap -> search -> verify -> merge -> commit)
- Persist only verified/fail-closed data into the target `vehicles` + `specs` tables
- Run in a single Lambda (SQS-triggered) without external stateful orchestrators

Message contract (SQS record body JSON):
{
  "request_id": "req-123",
  "market": "IN",
  "source": "firecrawl-gpt",
  "fetched_at": "2026-01-01T10:00:00Z",
  "source_url": "https://oem.example.com/model",

  "raw_extraction": {
    "make": "Tata",
    "model": "Nexon EV",
    "variant": "Creative+",
    "body_type": "SUV",
    "seating": 5,
    "status": "active",
    "specs": {
      "price_ex_showroom_inr": 1999000,
      "range_km": 465,
      "battery_kwh": 45,
      "motor_power_kw": 106,
      "motor_torque_nm": 215,
      "motor_type": "PMSM",
      "battery_type": "LFP",
      "charge_ac_kw": 7.2,
      "charge_dc_kw": 60,
      "charger_options": "3.3kW/7.2kW",
      "accel_0_100_s": 8.9
    },
    "notes": "variant dependent charging"
  },

  "enrichment_candidates": {
    "specs.range_km": [
      {
        "url": "https://oem.example.com/specs",
        "value": 465,
        "source_type": "oem_official"
      },
      {
        "url": "https://trusted-autospecs.example.net/specs",
        "value": 465,
        "source_type": "trusted_specs"
      }
    ]
  }
}
"""

from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Protocol, Sequence, Tuple, cast
from urllib.parse import urlparse


# -------- Multi-agent guardrail policy --------

REQUIRED_TOP_LEVEL_FIELDS: Sequence[str] = ("make", "model", "variant")
REQUIRED_SPEC_FIELDS: Sequence[str] = (
    "price_ex_showroom_inr",
    "range_km",
    "battery_kwh",
    "motor_power_kw",
    "motor_torque_nm",
    "charge_ac_kw",
    "charge_dc_kw",
)

SOURCE_PRIORITY: Dict[str, int] = {
    "oem_official": 100,
    "homologation": 90,
    "trusted_specs": 80,
    "other": 10,
}

STAGE_BUDGETS: Dict[str, Dict[str, int]] = {
    "PrimaryAgent": {"max_tokens": 4000, "timeout_ms": 12000, "max_retries": 1},
    "GapDetector": {"max_tokens": 1000, "timeout_ms": 2500, "max_retries": 0},
    "SearchAgent": {"max_tokens": 2000, "timeout_ms": 6000, "max_retries": 1},
    "VerifierAgent": {"max_tokens": 2000, "timeout_ms": 5000, "max_retries": 1},
    "MergerAgent": {"max_tokens": 1000, "timeout_ms": 3500, "max_retries": 0},
    "CommitAgent": {"max_tokens": 1000, "timeout_ms": 2500, "max_retries": 0},
}

MAX_HOPS = 2


def _trusted_domain_allowlist() -> List[str]:
    raw = os.getenv(
        "TRUSTED_DOMAIN_ALLOWLIST",
        "manufacturer.example.com,homologation.example.org,trusted-autospecs.example.net",
    )
    return [x.strip().lower() for x in raw.split(",") if x.strip()]


class StageValidationError(ValueError):
    pass


class DBConnection(Protocol):
    closed: Any

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def cursor(self) -> Any: ...


@dataclass
class Budget:
    timeout_ms: int
    max_tokens: int
    started_at: float = time.time()
    used_tokens: int = 0

    def assert_time(self) -> None:
        elapsed_ms = (time.time() - self.started_at) * 1000
        if elapsed_ms > self.timeout_ms:
            raise TimeoutError("time budget exceeded")

    def use_tokens(self, amount: int) -> None:
        self.used_tokens += amount
        if self.used_tokens > self.max_tokens:
            raise TimeoutError("token budget exceeded")


@dataclass(frozen=True)
class Evidence:
    field: str
    value: Any
    url: str
    source_type: str


def _strict_keys(data: Dict[str, Any], expected: Sequence[str], stage: str) -> None:
    incoming = set(data.keys())
    wanted = set(expected)
    if incoming != wanted:
        raise StageValidationError(
            f"{stage} schema mismatch; missing={sorted(wanted - incoming)}, extra={sorted(incoming - wanted)}"
        )


def _extract_domain(url: str) -> str:
    return (urlparse(url).hostname or "").lower()


def _is_allowed_url(url: str, allowlist: Sequence[str]) -> bool:
    domain = _extract_domain(url)
    return any(domain == allowed or domain.endswith(f".{allowed}") for allowed in allowlist)


# -------- Stage handlers --------

def run_primary_agent(message: Dict[str, Any], budget: Budget) -> Dict[str, Any]:
    budget.assert_time()
    budget.use_tokens(900)

    source_url = str(message.get("source_url") or "")
    raw = message.get("raw_extraction") or {}
    if not isinstance(raw, dict):
        raise ValueError("raw_extraction must be an object")

    specs = raw.get("specs") or {}
    if not isinstance(specs, dict):
        raise ValueError("raw_extraction.specs must be an object")

    fields: Dict[str, Any] = {
        "make": raw.get("make"),
        "model": raw.get("model"),
        "variant": raw.get("variant", ""),
        "body_type": raw.get("body_type"),
        "seating": raw.get("seating"),
        "status": raw.get("status", "active"),
        "notes": raw.get("notes"),
        "specs": {
            "price_ex_showroom_inr": specs.get("price_ex_showroom_inr"),
            "range_km": specs.get("range_km"),
            "battery_kwh": specs.get("battery_kwh"),
            "motor_power_kw": specs.get("motor_power_kw"),
            "motor_torque_nm": specs.get("motor_torque_nm"),
            "motor_type": specs.get("motor_type"),
            "battery_type": specs.get("battery_type"),
            "charge_ac_kw": specs.get("charge_ac_kw"),
            "charge_dc_kw": specs.get("charge_dc_kw"),
            "charger_options": specs.get("charger_options"),
            "accel_0_100_s": specs.get("accel_0_100_s"),
        },
    }

    output = {
        "source_url": source_url,
        "fields": fields,
        "provenance": {"source_url": source_url},
        "confidence": {},
    }
    _strict_keys(output, ("source_url", "fields", "provenance", "confidence"), "PrimaryAgent")
    return output


def run_gap_detector(primary_output: Dict[str, Any], budget: Budget) -> Dict[str, Any]:
    budget.assert_time()
    budget.use_tokens(150)

    _strict_keys(primary_output, ("source_url", "fields", "provenance", "confidence"), "GapDetector input")
    fields = primary_output["fields"]
    specs = fields.get("specs") or {}

    missing: List[str] = []
    for f in REQUIRED_TOP_LEVEL_FIELDS:
        if fields.get(f) in (None, ""):
            missing.append(f)
    for f in REQUIRED_SPEC_FIELDS:
        if specs.get(f) in (None, ""):
            missing.append(f"specs.{f}")

    output = {"missing_targets": missing, "reason_by_field": {k: "missing_from_primary" for k in missing}}
    _strict_keys(output, ("missing_targets", "reason_by_field"), "GapDetector output")
    return output


def run_search_agent(message: Dict[str, Any], missing_targets: Sequence[str], budget: Budget) -> Dict[str, Any]:
    budget.assert_time()
    budget.use_tokens(400)

    allowlist = _trusted_domain_allowlist()
    provided = message.get("enrichment_candidates") or {}
    if not isinstance(provided, dict):
        provided = {}

    candidates: Dict[str, List[Dict[str, Any]]] = {}
    for target in missing_targets:
        raw_candidates = provided.get(target) or []
        if not isinstance(raw_candidates, list):
            raw_candidates = []

        filtered: List[Dict[str, Any]] = []
        for item in raw_candidates:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or "")
            if not url or not _is_allowed_url(url, allowlist):
                continue
            source_type = str(item.get("source_type") or "other")
            filtered.append(
                {
                    "url": url,
                    "value": item.get("value"),
                    "source_type": source_type,
                    "domain": _extract_domain(url),
                }
            )
        candidates[target] = filtered

    output = {"candidates": candidates}
    _strict_keys(output, ("candidates",), "SearchAgent output")
    return output


def run_verifier_agent(candidates: Dict[str, List[Dict[str, Any]]], budget: Budget) -> Dict[str, Any]:
    budget.assert_time()
    budget.use_tokens(700)

    verified: Dict[str, Dict[str, Any]] = {}
    unresolved: Dict[str, str] = {}

    for field, values in candidates.items():
        if not values:
            unresolved[field] = "no_allowed_sources"
            continue

        groups: Dict[str, List[Evidence]] = {}
        for item in values:
            key = json.dumps(item.get("value"), sort_keys=True, default=str)
            groups.setdefault(key, []).append(
                Evidence(
                    field=field,
                    value=item.get("value"),
                    url=item["url"],
                    source_type=item.get("source_type", "other"),
                )
            )

        if len(groups) > 1:
            unresolved[field] = "contradictory_sources"
            continue

        group = next(iter(groups.values()))
        top_priority = max(SOURCE_PRIORITY.get(e.source_type, 0) for e in group)
        if len(group) < 2 and top_priority < 90:
            unresolved[field] = "insufficient_independent_sources"
            continue

        verified[field] = {
            "value": group[0].value,
            "evidence": [e.__dict__ for e in group],
            "confidence": min(0.95, 0.65 + 0.1 * len(group)),
        }

    output = {"verified": verified, "unresolved": unresolved}
    _strict_keys(output, ("verified", "unresolved"), "VerifierAgent output")
    return output


def _set_field(payload: Dict[str, Any], dotted: str, value: Any) -> None:
    if dotted.startswith("specs."):
        payload.setdefault("specs", {})[dotted.split(".", 1)[1]] = value
    else:
        payload[dotted] = value


def _get_field(payload: Dict[str, Any], dotted: str) -> Any:
    if dotted.startswith("specs."):
        return (payload.get("specs") or {}).get(dotted.split(".", 1)[1])
    return payload.get(dotted)


def run_merger_agent(primary_output: Dict[str, Any], verifier_output: Dict[str, Any], budget: Budget) -> Dict[str, Any]:
    budget.assert_time()
    budget.use_tokens(300)

    fields = json.loads(json.dumps(primary_output["fields"]))
    conflict_flags: Dict[str, bool] = {}

    for field, verified in verifier_output["verified"].items():
        old_value = _get_field(fields, field)
        new_value = verified["value"]
        conflict = old_value not in (None, "") and old_value != new_value
        conflict_flags[field] = conflict

        if conflict:
            _set_field(fields, field, None)
        else:
            _set_field(fields, field, new_value)

    output = {
        "fields": fields,
        "conflict_flags": conflict_flags,
        "unresolved": verifier_output["unresolved"],
    }
    _strict_keys(output, ("fields", "conflict_flags", "unresolved"), "MergerAgent output")
    return output


def run_commit_agent(merged: Dict[str, Any], budget: Budget) -> Dict[str, Any]:
    budget.assert_time()
    budget.use_tokens(150)

    fields = merged["fields"]
    unresolved = merged["unresolved"]
    conflict_flags = merged["conflict_flags"]

    null_reasons: Dict[str, str] = {}

    for name in REQUIRED_TOP_LEVEL_FIELDS:
        if conflict_flags.get(name):
            fields[name] = None
            null_reasons[name] = "conflict_detected_fail_closed"
        elif fields.get(name) in (None, ""):
            null_reasons[name] = unresolved.get(name, "unverified_or_missing")

    for name in REQUIRED_SPEC_FIELDS:
        dotted = f"specs.{name}"
        value = (fields.get("specs") or {}).get(name)
        if conflict_flags.get(dotted):
            (fields.setdefault("specs", {}))[name] = None
            null_reasons[dotted] = "conflict_detected_fail_closed"
        elif value in (None, ""):
            null_reasons[dotted] = unresolved.get(dotted, "unverified_or_missing")

    output = {"record": fields, "null_reasons": null_reasons, "status": "committed_with_explicit_nulls"}
    _strict_keys(output, ("record", "null_reasons", "status"), "CommitAgent output")
    return output


def _run_stage(stage: str, fn: Any, *args: Any) -> Any:
    spec = STAGE_BUDGETS[stage]
    last_error: Optional[Exception] = None
    for _ in range(spec["max_retries"] + 1):
        budget = Budget(timeout_ms=spec["timeout_ms"], max_tokens=spec["max_tokens"], started_at=time.time())
        try:
            return fn(*args, budget)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    if last_error:
        raise last_error
    raise RuntimeError(f"invalid stage runner state for {stage}")


def run_coordinator(message: Dict[str, Any]) -> Dict[str, Any]:
    primary = _run_stage("PrimaryAgent", run_primary_agent, message)
    hops = 0

    while True:
        gap = _run_stage("GapDetector", run_gap_detector, primary)
        if not gap["missing_targets"]:
            merged = {"fields": primary["fields"], "conflict_flags": {}, "unresolved": {}}
            return _run_stage("CommitAgent", run_commit_agent, merged)

        if hops >= MAX_HOPS:
            merged = {
                "fields": primary["fields"],
                "conflict_flags": {},
                "unresolved": {target: "max_hops_exceeded" for target in gap["missing_targets"]},
            }
            return _run_stage("CommitAgent", run_commit_agent, merged)

        search = _run_stage("SearchAgent", run_search_agent, message, gap["missing_targets"])
        verified = _run_stage("VerifierAgent", run_verifier_agent, search["candidates"])
        merged = _run_stage("MergerAgent", run_merger_agent, primary, verified)

        primary = {
            "source_url": primary["source_url"],
            "fields": merged["fields"],
            "provenance": primary["provenance"],
            "confidence": {},
        }
        hops += 1


# -------- Persistence --------


def _load_driver() -> tuple[Any, bool]:
    try:
        import psycopg as driver  # type: ignore

        return driver, True
    except Exception:  # noqa: BLE001
        import psycopg2 as driver  # type: ignore

        return driver, False


_DB_CONN: Optional[DBConnection] = None
_DB_DRIVER: Any = None
_IS_PSYCOPG3: Optional[bool] = None


def _connection_closed(conn: DBConnection) -> bool:
    closed = getattr(conn, "closed", 1)
    if isinstance(closed, bool):
        return closed
    return int(closed) != 0


def _get_connection() -> DBConnection:
    global _DB_CONN, _DB_DRIVER, _IS_PSYCOPG3

    if _DB_DRIVER is None or _IS_PSYCOPG3 is None:
        _DB_DRIVER, _IS_PSYCOPG3 = _load_driver()

    if _DB_CONN is not None and not _connection_closed(_DB_CONN):
        return _DB_CONN

    dsn = os.getenv("DB_DSN")
    if dsn:
        conn = _DB_DRIVER.connect(dsn)
    else:
        required = {
            "host": os.getenv("DB_HOST"),
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise RuntimeError(f"Missing DB config. Set DB_DSN or env vars for: {', '.join(missing)}")

        conn = _DB_DRIVER.connect(
            host=required["host"],
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=required["dbname"],
            user=required["user"],
            password=required["password"],
            sslmode=os.getenv("DB_SSLMODE", "require"),
        )

    _DB_CONN = cast(DBConnection, conn)
    return _DB_CONN


def persist_record(message: Dict[str, Any], committed: Dict[str, Any]) -> None:
    record = committed["record"]
    specs = record.get("specs") or {}

    market = str(message.get("market") or "IN")
    make = str(record.get("make") or "").strip()
    model = str(record.get("model") or "").strip()
    variant = str(record.get("variant") or "").strip()

    if not make or not model:
        raise ValueError("Cannot persist: make/model unresolved after guardrails")

    source_url = str(message.get("source_url") or "")
    if not source_url:
        raise ValueError("source_url is required")

    raw_json = json.dumps(message.get("raw_extraction") or {}, default=str)
    fetched_at = _iso_or_now(message.get("fetched_at"))

    conn = _get_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO vehicles (market, make, model, variant, body_type, seating, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (market, make, model, variant)
            DO UPDATE SET
              body_type = EXCLUDED.body_type,
              seating = EXCLUDED.seating,
              status = EXCLUDED.status,
              updated_at = now()
            RETURNING id
            """,
            (
                market,
                make,
                model,
                variant,
                record.get("body_type"),
                record.get("seating"),
                record.get("status") or "active",
            ),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("Could not resolve vehicle id")
        vehicle_id = row[0]

        cur.execute(
            """
            INSERT INTO specs (
              vehicle_id,
              price_ex_showroom_inr,
              range_km,
              battery_kwh,
              motor_power_kw,
              motor_torque_nm,
              motor_type,
              battery_type,
              charge_ac_kw,
              charge_dc_kw,
              charger_options,
              accel_0_100_s,
              source_url,
              fetched_at,
              raw_json,
              notes
            ) VALUES (
              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s
            )
            ON CONFLICT (vehicle_id)
            DO UPDATE SET
              price_ex_showroom_inr = EXCLUDED.price_ex_showroom_inr,
              range_km = EXCLUDED.range_km,
              battery_kwh = EXCLUDED.battery_kwh,
              motor_power_kw = EXCLUDED.motor_power_kw,
              motor_torque_nm = EXCLUDED.motor_torque_nm,
              motor_type = EXCLUDED.motor_type,
              battery_type = EXCLUDED.battery_type,
              charge_ac_kw = EXCLUDED.charge_ac_kw,
              charge_dc_kw = EXCLUDED.charge_dc_kw,
              charger_options = EXCLUDED.charger_options,
              accel_0_100_s = EXCLUDED.accel_0_100_s,
              source_url = EXCLUDED.source_url,
              fetched_at = EXCLUDED.fetched_at,
              raw_json = EXCLUDED.raw_json,
              notes = EXCLUDED.notes
            """,
            (
                vehicle_id,
                specs.get("price_ex_showroom_inr"),
                specs.get("range_km"),
                specs.get("battery_kwh"),
                specs.get("motor_power_kw"),
                specs.get("motor_torque_nm"),
                specs.get("motor_type"),
                specs.get("battery_type"),
                specs.get("charge_ac_kw"),
                specs.get("charge_dc_kw"),
                specs.get("charger_options"),
                specs.get("accel_0_100_s"),
                source_url,
                fetched_at,
                raw_json,
                record.get("notes"),
            ),
        )
    conn.commit()


def _iso_or_now(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, str) and value.strip():
        return value
    return datetime.now(timezone.utc).isoformat()


def _parse_record_body(record: Dict[str, Any]) -> Dict[str, Any]:
    body = record.get("body")
    if isinstance(body, str):
        payload = json.loads(body)
    elif isinstance(body, dict):
        payload = body
    else:
        raise ValueError("SQS record body must be JSON object")

    if not isinstance(payload, dict):
        raise ValueError("SQS record body must decode to object")
    return cast(Dict[str, Any], payload)


def _process_one_message(message: Dict[str, Any]) -> Dict[str, Any]:
    committed = run_coordinator(message)
    persist_record(message, committed)
    return committed


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    """SQS-compatible handler with partial failures.

    Deployment mode:
    - Configure Lambda trigger as SQS
    - Enable report batch item failures on event source mapping
    """
    records = event.get("Records")
    if isinstance(records, list):
        failures = []
        for rec in records:
            message_id = str(rec.get("messageId") or "unknown")
            try:
                message = _parse_record_body(rec)
                result = _process_one_message(message)
                print(json.dumps({"level": "INFO", "event": "record.processed", "message_id": message_id, "status": result["status"]}))
            except Exception as exc:  # noqa: BLE001
                print(json.dumps({"level": "ERROR", "event": "record.failed", "message_id": message_id, "error": str(exc)}))
                failures.append({"itemIdentifier": message_id})
        return {"batchItemFailures": failures}

    # direct invocation for quick local testing
    result = _process_one_message(event)
    return {"statusCode": 200, "body": json.dumps(result)}
