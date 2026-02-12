from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Protocol
import re


MAKE_ALIASES = {
    "vw": "Volkswagen",
    "merc": "Mercedes-Benz",
    "mercedes": "Mercedes-Benz",
    "bmw ag": "BMW",
    "chevy": "Chevrolet",
}

MODEL_ALIASES = {
    "model 3 performance": "Model 3",
    "f150": "F-150",
    "ioniq5": "Ioniq 5",
}

NUMERIC_UNITS = {
    "horsepower_kw": {"kw", "kilowatt", "kilowatts"},
    "torque_nm": {"nm", "n-m", "newton meter", "newton-meters"},
    "battery_kwh": {"kwh", "kilowatt-hour", "kilowatt-hours"},
    "range_km": {"km", "kilometer", "kilometers"},
    "acceleration_0_100_s": {"s", "sec", "seconds"},
    "top_speed_kmh": {"km/h", "kph", "kmh"},
    "curb_weight_kg": {"kg", "kilogram", "kilograms"},
    "price_usd": {"usd", "$", "dollar", "dollars"},
}

NUMERIC_BOUNDS: dict[str, tuple[float, float]] = {
    "horsepower_kw": (5, 1500),
    "torque_nm": (20, 3000),
    "battery_kwh": (1, 300),
    "range_km": (10, 2000),
    "acceleration_0_100_s": (1.0, 30.0),
    "top_speed_kmh": (20, 450),
    "curb_weight_kg": (400, 6000),
    "price_usd": (1000, 5000000),
}


class ValidationException(ValueError):
    def __init__(self, scope: str, errors: list[dict[str, Any]]):
        super().__init__(f"Validation failed for {scope}")
        self.scope = scope
        self.errors = errors


class DeadLetterQueue(Protocol):
    def enqueue(self, message: dict[str, Any]) -> None:
        """Send malformed payloads to ops review."""


class InMemoryDeadLetterQueue:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def enqueue(self, message: dict[str, Any]) -> None:
        self.messages.append(message)


def normalize_make_alias(make: str) -> str:
    key = make.strip().lower()
    return MAKE_ALIASES.get(key, make.strip().title())


def normalize_model_alias(model: str) -> str:
    cleaned = re.sub(r"\s+", " ", model.strip())
    key = cleaned.lower().replace("-", "")
    return MODEL_ALIASES.get(key, cleaned)


def normalize_variant_name(variant: str | None) -> str | None:
    if not variant:
        return None
    cleaned = re.sub(r"\s+", " ", variant.strip())
    cleaned = re.sub(r"\bawd\b", "All-Wheel Drive", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\brwd\b", "Rear-Wheel Drive", cleaned, flags=re.IGNORECASE)
    return cleaned.title().replace("All-Wheel Drive", "All-Wheel Drive").replace("Rear-Wheel Drive", "Rear-Wheel Drive")


def parse_numeric_string(value: Any, field_name: str) -> float:
    if isinstance(value, (int, float)):
        numeric = float(value)
    elif isinstance(value, str):
        raw = value.strip().lower().replace(",", "")
        raw = re.sub(r"\(.*?\)", "", raw).strip()

        units = NUMERIC_UNITS[field_name]
        for unit in sorted(units, key=len, reverse=True):
            raw = re.sub(rf"\b{re.escape(unit)}\b", "", raw).strip()

        raw = raw.replace("$", "")
        match = re.search(r"-?\d+(\.\d+)?", raw)
        if not match:
            raise ValueError(f"Could not parse numeric value from '{value}'")
        numeric = float(match.group(0))
    else:
        raise TypeError(f"Unsupported numeric value type: {type(value).__name__}")

    low, high = NUMERIC_BOUNDS[field_name]
    if not (low <= numeric <= high):
        raise ValueError(
            f"{field_name}={numeric} outside strict range [{low}, {high}]"
        )

    return numeric


def _expect_string(data: dict[str, Any], field: str) -> str:
    value = data.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


@dataclass(frozen=True)
class SpecsExtraction:
    horsepower_kw: float | None = None
    torque_nm: float | None = None
    battery_kwh: float | None = None
    range_km: float | None = None
    acceleration_0_100_s: float | None = None
    top_speed_kmh: float | None = None
    curb_weight_kg: float | None = None
    price_usd: float | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SpecsExtraction":
        allowed = {
            "horsepower_kw",
            "torque_nm",
            "battery_kwh",
            "range_km",
            "acceleration_0_100_s",
            "zero_to_hundred_s",
            "top_speed_kmh",
            "curb_weight_kg",
            "price_usd",
        }
        unknown = sorted(set(data) - allowed)
        if unknown:
            raise ValueError(f"Unknown spec fields: {unknown}")

        values: dict[str, float | None] = {}
        for field in NUMERIC_BOUNDS:
            source_field = "zero_to_hundred_s" if field == "acceleration_0_100_s" else field
            raw = data.get(source_field)
            values[field] = None if raw is None else parse_numeric_string(raw, field)
        return cls(**values)


@dataclass(frozen=True)
class VehicleExtraction:
    source_id: str
    make: str
    model: str
    variant: str | None
    model_year: int | None
    body_type: str | None
    specs: SpecsExtraction

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "VehicleExtraction":
        required = {"source_id", "make", "model", "specs"}
        allowed = required | {"variant", "model_year", "body_type"}
        missing = sorted(required - set(data))
        if missing:
            raise ValueError(f"Missing fields: {missing}")

        unknown = sorted(set(data) - allowed)
        if unknown:
            raise ValueError(f"Unknown vehicle fields: {unknown}")

        source_id = _expect_string(data, "source_id")
        make = normalize_make_alias(_expect_string(data, "make"))
        model = normalize_model_alias(_expect_string(data, "model"))

        variant_raw = data.get("variant")
        if variant_raw is not None and not isinstance(variant_raw, str):
            raise ValueError("variant must be a string when provided")
        variant = normalize_variant_name(variant_raw)

        model_year = data.get("model_year")
        if model_year is not None:
            if isinstance(model_year, str) and model_year.isdigit():
                model_year = int(model_year)
            if not isinstance(model_year, int):
                raise ValueError("model_year must be an int when provided")
            if not 1950 <= model_year <= 2100:
                raise ValueError("model_year out of range [1950, 2100]")

        body_type = data.get("body_type")
        if body_type is not None and not isinstance(body_type, str):
            raise ValueError("body_type must be a string when provided")

        specs_data = data.get("specs")
        if not isinstance(specs_data, dict):
            raise ValueError("specs must be an object")
        specs = SpecsExtraction.from_dict(specs_data)

        return cls(
            source_id=source_id,
            make=make,
            model=model,
            variant=variant,
            model_year=model_year,
            body_type=body_type,
            specs=specs,
        )


@dataclass(frozen=True)
class ExtractionEnvelope:
    request_id: str
    generated_at: datetime
    producer: str
    vehicles: list[VehicleExtraction]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExtractionEnvelope":
        allowed = {"request_id", "generated_at", "producer", "vehicles"}
        unknown = sorted(set(data) - allowed)
        if unknown:
            raise ValueError(f"Unknown envelope fields: {unknown}")

        request_id = _expect_string(data, "request_id")
        producer = _expect_string(data, "producer")

        generated_at_raw = data.get("generated_at")
        if not isinstance(generated_at_raw, str):
            raise ValueError("generated_at must be an ISO datetime string")
        generated_at = datetime.fromisoformat(generated_at_raw.replace("Z", "+00:00"))

        vehicles = data.get("vehicles")
        if not isinstance(vehicles, list):
            raise ValueError("vehicles must be a list")

        return cls(
            request_id=request_id,
            generated_at=generated_at,
            producer=producer,
            vehicles=vehicles,
        )


def _emit_dead_letter(
    queue: DeadLetterQueue,
    payload: dict[str, Any],
    errors: Iterable[Any],
    scope: str,
) -> None:
    queue.enqueue(
        {
            "scope": scope,
            "payload": payload,
            "errors": list(errors),
            "timestamp": datetime.utcnow().isoformat(),
        }
    )


def validate_and_canonicalize_extraction(
    payload: dict[str, Any],
    dead_letter_queue: DeadLetterQueue,
) -> ExtractionEnvelope:
    try:
        parsed_header = ExtractionEnvelope.from_dict(payload)
    except Exception as exc:  # noqa: BLE001
        errors = [{"error": str(exc)}]
        _emit_dead_letter(dead_letter_queue, payload, errors, scope="envelope")
        raise ValidationException("envelope", errors) from exc

    valid_vehicles: list[VehicleExtraction] = []
    for vehicle_payload in parsed_header.vehicles:
        try:
            if not isinstance(vehicle_payload, dict):
                raise ValueError("vehicle entry must be an object")
            validated = VehicleExtraction.from_dict(vehicle_payload)
            valid_vehicles.append(validated)
        except Exception as exc:  # noqa: BLE001
            _emit_dead_letter(
                dead_letter_queue,
                vehicle_payload if isinstance(vehicle_payload, dict) else {"raw": vehicle_payload},
                [{"error": str(exc)}],
                scope="vehicle",
            )

    if not valid_vehicles:
        errors = [{"error": "No valid vehicles after row-level validation"}]
        _emit_dead_letter(dead_letter_queue, payload, errors, scope="envelope")
        raise ValidationException("envelope", errors)

    return ExtractionEnvelope(
        request_id=parsed_header.request_id,
        generated_at=parsed_header.generated_at,
        producer=parsed_header.producer,
        vehicles=valid_vehicles,
    )
