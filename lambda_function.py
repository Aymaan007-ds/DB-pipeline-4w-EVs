"""Coordinator workflow for resilient vehicle-spec extraction.

This module supports two deployment modes:
1. Single Lambda orchestration (default): execute all agents in-process.
2. Step Functions orchestration: use exported ASL in step_functions/coordinator.asl.json
   and call individual stage handlers in this file.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse


REQUIRED_FIELDS: Sequence[str] = (
    "make",
    "model",
    "model_year",
    "trim",
    "engine",
    "transmission",
    "horsepower_hp",
    "torque_nm",
    "drivetrain",
)

TRUSTED_DOMAIN_ALLOWLIST: Sequence[str] = (
    "manufacturer.example.com",  # replace with actual OEM domains
    "homologation.example.org",  # replace with official homologation docs hosts
    "trusted-autospecs.example.net",  # replace with curated trusted spec providers
)

SOURCE_PRIORITY: Dict[str, int] = {
    "oem_official": 100,
    "homologation": 90,
    "trusted_specs": 80,
    "other": 10,
}

STAGE_BUDGETS: Dict[str, Dict[str, int]] = {
    "PrimaryAgent": {"max_tokens": 4_000, "timeout_ms": 12_000, "max_retries": 1},
    "GapDetector": {"max_tokens": 800, "timeout_ms": 2_000, "max_retries": 0},
    "SearchAgent": {"max_tokens": 2_000, "timeout_ms": 6_000, "max_retries": 2},
    "VerifierAgent": {"max_tokens": 2_000, "timeout_ms": 4_000, "max_retries": 1},
    "MergerAgent": {"max_tokens": 1_000, "timeout_ms": 3_000, "max_retries": 0},
    "CommitAgent": {"max_tokens": 500, "timeout_ms": 2_000, "max_retries": 0},
}

MAX_HOPS = 2


class StageValidationError(ValueError):
    """Raised when stage input/output breaks strict schema rules."""


class ContradictoryDataError(RuntimeError):
    """Raised when contradictory data is detected in fail-closed mode."""


@dataclass
class SourceEvidence:
    url: str
    domain: str
    source_type: str
    value: Any


class Budget:
    def __init__(self, timeout_ms: int, max_tokens: int) -> None:
        self.started_at = time.time()
        self.timeout_ms = timeout_ms
        self.max_tokens = max_tokens
        self.tokens_used = 0

    def consume_tokens(self, tokens: int) -> None:
        self.tokens_used += tokens
        if self.tokens_used > self.max_tokens:
            raise TimeoutError("Token budget exceeded")

    def assert_time_budget(self) -> None:
        elapsed_ms = (time.time() - self.started_at) * 1000
        if elapsed_ms > self.timeout_ms:
            raise TimeoutError("Time budget exceeded")


def _strict_require_keys(data: Dict[str, Any], keys: Sequence[str], stage: str) -> None:
    incoming = set(data.keys())
    expected = set(keys)
    missing = expected - incoming
    extra = incoming - expected
    if missing or extra:
        raise StageValidationError(
            f"{stage} schema mismatch; missing={sorted(missing)}, extra={sorted(extra)}"
        )


def _extract_domain(url: str) -> str:
    return (urlparse(url).hostname or "").lower()


def _is_allowed_domain(url: str, allowlist: Sequence[str]) -> bool:
    domain = _extract_domain(url)
    return any(domain == d or domain.endswith(f".{d}") for d in allowlist)


def _source_type_for_domain(domain: str) -> str:
    if "manufacturer" in domain or domain.endswith(".oem.com"):
        return "oem_official"
    if "homolog" in domain:
        return "homologation"
    if "autospec" in domain:
        return "trusted_specs"
    return "other"


def run_primary_agent(source_url: str, budget: Budget) -> Dict[str, Any]:
    """Stub for Firecrawl+GPT extraction with strict output schema."""
    budget.assert_time_budget()
    budget.consume_tokens(1_200)

    # Placeholder extraction output; replace with actual Firecrawl + LLM call.
    extracted = {
        "make": "Example Motors",
        "model": "Falcon",
        "model_year": 2024,
        "trim": None,
        "engine": "2.0L Turbo",
        "transmission": None,
        "horsepower_hp": 195,
        "torque_nm": None,
        "drivetrain": "AWD",
    }

    output = {
        "source_url": source_url,
        "fields": extracted,
        "provenance": {field: [source_url] for field in extracted if extracted[field] is not None},
        "confidence": {field: 0.7 for field in extracted if extracted[field] is not None},
    }
    _strict_require_keys(output, ("source_url", "fields", "provenance", "confidence"), "PrimaryAgent")
    return output


def run_gap_detector(primary_output: Dict[str, Any], budget: Budget) -> Dict[str, Any]:
    budget.assert_time_budget()
    budget.consume_tokens(200)
    _strict_require_keys(primary_output, ("source_url", "fields", "provenance", "confidence"), "GapDetector input")

    fields = primary_output["fields"]
    missing = [f for f in REQUIRED_FIELDS if fields.get(f) in (None, "", [])]

    output = {
        "missing_targets": missing,
        "reason_by_field": {f: "missing_from_primary" for f in missing},
    }
    _strict_require_keys(output, ("missing_targets", "reason_by_field"), "GapDetector output")
    return output


def run_search_agent(missing_targets: Sequence[str], allowlist: Sequence[str], budget: Budget) -> Dict[str, Any]:
    budget.assert_time_budget()
    budget.consume_tokens(800)

    results: Dict[str, List[Dict[str, Any]]] = {}
    for field in missing_targets:
        # Placeholder candidates; replace with constrained search API calls.
        candidate_urls = [
            "https://manufacturer.example.com/specs/falcon-2024",
            "https://trusted-autospecs.example.net/falcon-2024",
        ]
        filtered = [u for u in candidate_urls if _is_allowed_domain(u, allowlist)]
        results[field] = [
            {
                "url": url,
                "domain": _extract_domain(url),
                "source_type": _source_type_for_domain(_extract_domain(url)),
                "value": "placeholder-value",
            }
            for url in filtered
        ]

    output = {"candidates": results}
    _strict_require_keys(output, ("candidates",), "SearchAgent output")
    return output


def run_verifier_agent(candidates: Dict[str, List[Dict[str, Any]]], budget: Budget) -> Dict[str, Any]:
    budget.assert_time_budget()
    budget.consume_tokens(700)

    verified: Dict[str, Dict[str, Any]] = {}
    unresolved: Dict[str, str] = {}

    for field, values in candidates.items():
        if not values:
            unresolved[field] = "no_allowed_sources"
            continue

        grouped: Dict[Any, List[SourceEvidence]] = {}
        for item in values:
            evidence = SourceEvidence(
                url=item["url"],
                domain=item["domain"],
                source_type=item["source_type"],
                value=item["value"],
            )
            grouped.setdefault(evidence.value, []).append(evidence)

        # fail-closed: contradictory values unresolved.
        if len(grouped) > 1:
            unresolved[field] = "contradictory_sources"
            continue

        value, evidences = next(iter(grouped.items()))
        if len(evidences) < 2 and max(SOURCE_PRIORITY.get(e.source_type, 0) for e in evidences) < 90:
            unresolved[field] = "insufficient_independent_sources"
            continue

        verified[field] = {
            "value": value,
            "evidence": [e.__dict__ for e in evidences],
            "confidence": min(0.95, 0.65 + 0.1 * len(evidences)),
        }

    output = {"verified": verified, "unresolved": unresolved}
    _strict_require_keys(output, ("verified", "unresolved"), "VerifierAgent output")
    return output


def run_merger_agent(primary_output: Dict[str, Any], verifier_output: Dict[str, Any], budget: Budget) -> Dict[str, Any]:
    budget.assert_time_budget()
    budget.consume_tokens(300)

    fields = dict(primary_output["fields"])
    provenance = {k: list(v) for k, v in primary_output["provenance"].items()}
    confidence = dict(primary_output["confidence"])
    conflict_flags: Dict[str, bool] = {}

    for field, verified in verifier_output["verified"].items():
        prior = fields.get(field)
        new_value = verified["value"]
        conflict = prior not in (None, "", []) and prior != new_value
        conflict_flags[field] = conflict

        if conflict:
            # fail-closed: keep null and mark unresolved instead of overriding.
            fields[field] = None
            confidence[field] = 0.0
            provenance[field] = [e["url"] for e in verified["evidence"]]
        else:
            fields[field] = new_value
            confidence[field] = verified["confidence"]
            provenance[field] = [e["url"] for e in verified["evidence"]]

    output = {
        "fields": fields,
        "provenance": provenance,
        "confidence": confidence,
        "conflict_flags": conflict_flags,
        "unresolved": verifier_output["unresolved"],
    }
    _strict_require_keys(output, ("fields", "provenance", "confidence", "conflict_flags", "unresolved"), "MergerAgent output")
    return output


def run_commit_agent(merged: Dict[str, Any], budget: Budget) -> Dict[str, Any]:
    budget.assert_time_budget()
    budget.consume_tokens(200)

    committed: Dict[str, Any] = {}
    null_reasons: Dict[str, str] = {}

    for field in REQUIRED_FIELDS:
        value = merged["fields"].get(field)
        field_confidence = merged["confidence"].get(field, 0)
        conflicted = merged["conflict_flags"].get(field, False)

        if conflicted:
            committed[field] = None
            null_reasons[field] = "conflict_detected_fail_closed"
        elif field in merged["unresolved"]:
            committed[field] = None
            null_reasons[field] = merged["unresolved"][field]
        elif value is None or field_confidence < 0.6:
            committed[field] = None
            null_reasons[field] = "unverified_or_low_confidence"
        else:
            committed[field] = value

    output = {
        "record": committed,
        "null_reasons": null_reasons,
        "status": "committed_with_explicit_nulls",
    }
    _strict_require_keys(output, ("record", "null_reasons", "status"), "CommitAgent output")
    return output


def _run_stage_with_retry(stage_name: str, fn, *args):
    spec = STAGE_BUDGETS[stage_name]
    retries = spec["max_retries"]
    last_error: Optional[Exception] = None
    for _ in range(retries + 1):
        budget = Budget(timeout_ms=spec["timeout_ms"], max_tokens=spec["max_tokens"])
        try:
            return fn(*args, budget)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
    if last_error:
        raise last_error
    raise RuntimeError(f"Unexpected stage runner state for {stage_name}")


def run_coordinator(source_url: str) -> Dict[str, Any]:
    hops = 0
    primary = _run_stage_with_retry("PrimaryAgent", run_primary_agent, source_url)

    while True:
        gap = _run_stage_with_retry("GapDetector", run_gap_detector, primary)
        if not gap["missing_targets"]:
            merged = {
                "fields": primary["fields"],
                "provenance": primary["provenance"],
                "confidence": primary["confidence"],
                "conflict_flags": {},
                "unresolved": {},
            }
            return _run_stage_with_retry("CommitAgent", run_commit_agent, merged)

        if hops >= MAX_HOPS:
            unresolved = {f: "max_hops_exceeded" for f in gap["missing_targets"]}
            merged = {
                "fields": primary["fields"],
                "provenance": primary["provenance"],
                "confidence": primary["confidence"],
                "conflict_flags": {},
                "unresolved": unresolved,
            }
            return _run_stage_with_retry("CommitAgent", run_commit_agent, merged)

        search = _run_stage_with_retry(
            "SearchAgent", run_search_agent, gap["missing_targets"], TRUSTED_DOMAIN_ALLOWLIST
        )
        verified = _run_stage_with_retry("VerifierAgent", run_verifier_agent, search["candidates"])
        merged = _run_stage_with_retry("MergerAgent", run_merger_agent, primary, verified)

        # merge becomes new primary context for possible further enrichment hops.
        primary = {
            "source_url": source_url,
            "fields": merged["fields"],
            "provenance": merged["provenance"],
            "confidence": merged["confidence"],
        }
        hops += 1


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    # strict input schema
    _strict_require_keys(event, ("source_url",), "Lambda input")
    result = run_coordinator(event["source_url"])
    return {"statusCode": 200, "body": json.dumps(result)}


# Step Functions task wrappers

def primary_agent_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    return _run_stage_with_retry("PrimaryAgent", run_primary_agent, event["source_url"])


def gap_detector_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    return _run_stage_with_retry("GapDetector", run_gap_detector, event)


def search_agent_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    return _run_stage_with_retry(
        "SearchAgent", run_search_agent, event["missing_targets"], TRUSTED_DOMAIN_ALLOWLIST
    )


def verifier_agent_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    return _run_stage_with_retry("VerifierAgent", run_verifier_agent, event["candidates"])


def merger_agent_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    return _run_stage_with_retry("MergerAgent", run_merger_agent, event["primary"], event["verifier"])


def commit_agent_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    return _run_stage_with_retry("CommitAgent", run_commit_agent, event)
