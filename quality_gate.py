"""Post-extraction validation and routing quality gate.

Use this module immediately after ``extract_with_gpt(...)`` to:
1) evaluate required/optional `specs` field policy,
2) compute completeness,
3) bucket confidence,
4) decide whether to UPSERT directly or route to enrichment,
5) persist machine-readable diagnostics with the extraction payload.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


DEFAULT_SPEC_WEIGHTS: dict[str, float] = {
    "make": 2.0,
    "model": 2.0,
    "variant": 1.5,
    "year": 1.5,
    "body_type": 1.0,
    "fuel_type": 1.0,
    "transmission": 1.0,
    "drivetrain": 1.0,
    "engine_displacement": 0.8,
    "power_hp": 0.8,
    "torque_nm": 0.6,
    "doors": 0.5,
    "seats": 0.5,
    "mileage_km": 0.7,
    "price": 1.2,
    "currency": 1.0,
}


DEFAULT_REQUIRED_FIELDS: set[str] = {
    "make",
    "model",
    "variant",
    "year",
    "price",
    "currency",
}


DEFAULT_OPTIONAL_FIELDS: set[str] = {
    "body_type",
    "fuel_type",
    "transmission",
    "drivetrain",
    "engine_displacement",
    "power_hp",
    "torque_nm",
    "doors",
    "seats",
    "mileage_km",
}


DEFAULT_POLICY_REGISTRY: dict[str, dict[str, set[str]]] = {
    "market:EU": {
        "required": {"make", "model", "variant", "year", "price", "currency", "mileage_km"},
        "optional": {"fuel_type", "transmission", "drivetrain", "power_hp"},
    },
    "market:US": {
        "required": {"make", "model", "variant", "year", "price", "currency"},
        "optional": {"fuel_type", "transmission", "drivetrain", "body_type", "doors"},
    },
    "publisher:premium_oem": {
        "required": {"make", "model", "variant", "year", "price", "currency", "fuel_type", "transmission"},
        "optional": {"drivetrain", "power_hp", "torque_nm", "engine_displacement"},
    },
}


_AMBIGUOUS_MARKERS = {
    "unknown",
    "n/a",
    "na",
    "none",
    "null",
    "tbd",
    "unspecified",
    "other",
    "varies",
}


@dataclass(frozen=True)
class FieldPolicy:
    required: set[str]
    optional: set[str]


@dataclass(frozen=True)
class CompletenessResult:
    score: float
    missing_fields: list[str]
    ambiguous_fields: list[str]
    populated_weight: float
    total_weight: float


@dataclass(frozen=True)
class IdentityCertainty:
    certain: bool
    missing: list[str]
    ambiguous: list[str]


def resolve_field_policy(
    *,
    market: str | None = None,
    publisher_profile: str | None = None,
    policy_registry: Mapping[str, Mapping[str, set[str]]] | None = None,
) -> FieldPolicy:
    """Resolve required/optional specs fields by market/publisher profile."""
    registry = dict(policy_registry or DEFAULT_POLICY_REGISTRY)

    required = set(DEFAULT_REQUIRED_FIELDS)
    optional = set(DEFAULT_OPTIONAL_FIELDS)

    for key in (f"market:{market}" if market else None, f"publisher:{publisher_profile}" if publisher_profile else None):
        if not key or key not in registry:
            continue
        required |= set(registry[key].get("required", set()))
        optional |= set(registry[key].get("optional", set()))

    optional -= required
    return FieldPolicy(required=required, optional=optional)


def _is_populated(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        cleaned = value.strip().lower()
        return bool(cleaned) and cleaned not in _AMBIGUOUS_MARKERS
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    return True


def _is_ambiguous(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        cleaned = value.strip().lower()
        if not cleaned:
            return False
        if cleaned in _AMBIGUOUS_MARKERS:
            return True
        return "/" in cleaned or " or " in cleaned
    return False


def compute_completeness(
    specs: Mapping[str, Any],
    *,
    policy: FieldPolicy,
    field_weights: Mapping[str, float] | None = None,
) -> CompletenessResult:
    """Calculate weighted non-null ratio + missing/ambiguous diagnostics."""
    weights = dict(DEFAULT_SPEC_WEIGHTS)
    if field_weights:
        weights.update(field_weights)

    scoped_fields = sorted(policy.required | policy.optional)
    total_weight = sum(weights.get(field, 1.0) for field in scoped_fields)
    populated_weight = 0.0
    missing_fields: list[str] = []
    ambiguous_fields: list[str] = []

    for field in scoped_fields:
        value = specs.get(field)
        if _is_ambiguous(value):
            ambiguous_fields.append(field)

        if _is_populated(value):
            populated_weight += weights.get(field, 1.0)
        elif field in policy.required:
            missing_fields.append(field)

    score = 0.0 if total_weight == 0 else round(populated_weight / total_weight, 4)
    return CompletenessResult(
        score=score,
        missing_fields=missing_fields,
        ambiguous_fields=sorted(set(ambiguous_fields)),
        populated_weight=round(populated_weight, 4),
        total_weight=round(total_weight, 4),
    )


def assess_identity_certainty(specs: Mapping[str, Any]) -> IdentityCertainty:
    """Evaluate certainty for make/model/variant identity trio."""
    identity_fields = ("make", "model", "variant")
    missing: list[str] = []
    ambiguous: list[str] = []

    for field in identity_fields:
        value = specs.get(field)
        if not _is_populated(value):
            missing.append(field)
        elif _is_ambiguous(value):
            ambiguous.append(field)

    return IdentityCertainty(
        certain=(not missing and not ambiguous),
        missing=missing,
        ambiguous=ambiguous,
    )


def confidence_bucket(score: float, identity: IdentityCertainty) -> str:
    """Map completeness + identity certainty to high/medium/low confidence."""
    if score >= 0.85 and identity.certain:
        return "high"
    if score >= 0.65 and len(identity.missing) <= 1 and len(identity.ambiguous) <= 1:
        return "medium"
    return "low"


def gate_action(bucket: str) -> str:
    """Route by gate rule: high => direct upsert, medium/low => enrichment."""
    return "direct_upsert" if bucket == "high" else "route_to_enrichment"


def quality_gate(
    extraction_payload: Mapping[str, Any],
    *,
    market: str | None = None,
    publisher_profile: str | None = None,
    policy_registry: Mapping[str, Mapping[str, set[str]]] | None = None,
    field_weights: Mapping[str, float] | None = None,
) -> dict[str, Any]:
    """Run post-extraction validation and attach diagnostics for observability.

    Parameters
    ----------
    extraction_payload:
        The output object from ``extract_with_gpt(...)`` expected to contain a
        top-level ``specs`` mapping.

    Returns
    -------
    dict
        New payload with a ``quality_gate`` section containing machine-readable
        diagnostics and routing decision.
    """
    specs = dict(extraction_payload.get("specs", {}))
    policy = resolve_field_policy(
        market=market,
        publisher_profile=publisher_profile,
        policy_registry=policy_registry,
    )

    completeness = compute_completeness(specs, policy=policy, field_weights=field_weights)
    identity = assess_identity_certainty(specs)
    bucket = confidence_bucket(completeness.score, identity)

    diagnostics = {
        "missing_fields": completeness.missing_fields,
        "ambiguous_fields": sorted(set(completeness.ambiguous_fields + identity.ambiguous)),
        "score": completeness.score,
        "score_breakdown": {
            "populated_weight": completeness.populated_weight,
            "total_weight": completeness.total_weight,
        },
        "identity": {
            "certain": identity.certain,
            "missing": identity.missing,
            "ambiguous": identity.ambiguous,
        },
        "policy": {
            "required_fields": sorted(policy.required),
            "optional_fields": sorted(policy.optional),
            "market": market,
            "publisher_profile": publisher_profile,
        },
    }

    return {
        **dict(extraction_payload),
        "quality_gate": {
            "confidence_bucket": bucket,
            "gate_action": gate_action(bucket),
            "diagnostics": diagnostics,
        },
    }
