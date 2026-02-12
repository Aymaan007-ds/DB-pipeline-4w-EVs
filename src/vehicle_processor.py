from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Protocol


class Logger(Protocol):
    def info(self, event: str, **fields: Any) -> None: ...

    def error(self, event: str, **fields: Any) -> None: ...


class IdempotencyStore(Protocol):
    def seen(self, key: str) -> bool: ...

    def remember(self, key: str) -> None: ...


class DlqClient(Protocol):
    def send(self, payload: Dict[str, Any]) -> None: ...


class VehiclePersister(Protocol):
    @contextmanager
    def transaction(self) -> Iterable[None]: ...

    def upsert(self, vehicle: Dict[str, Any], context: Dict[str, Any]) -> None: ...


@dataclass(frozen=True)
class ProcessingSummary:
    written: int
    failed: int
    skipped: int


class InMemoryIdempotencyStore:
    def __init__(self) -> None:
        self._keys: set[str] = set()

    def seen(self, key: str) -> bool:
        return key in self._keys

    def remember(self, key: str) -> None:
        self._keys.add(key)


class VehicleMessageProcessor:
    """
    Message-level context is preserved, while each vehicle is written in its own
    transaction boundary to reduce blast radius.
    """

    def __init__(
        self,
        persister: VehiclePersister,
        idempotency_store: IdempotencyStore,
        dlq_client: DlqClient,
        logger: Logger,
        *,
        idempotency_window_minutes: int = 30,
        validate: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self._persister = persister
        self._idempotency_store = idempotency_store
        self._dlq_client = dlq_client
        self._logger = logger
        self._idempotency_window_minutes = idempotency_window_minutes
        self._validate = validate or self._default_validate

    def process_message(self, message: Dict[str, Any]) -> ProcessingSummary:
        context = {
            "market": message.get("market"),
            "source": message.get("source"),
            "fetched_at": message.get("fetched_at"),
            "request_id": message.get("request_id"),
        }

        written = failed = skipped = 0

        vehicles = message.get("vehicles") or []
        for vehicle in vehicles:
            idem_key = self._idempotency_key(context, vehicle)
            if self._idempotency_store.seen(idem_key):
                skipped += 1
                self._logger.info(
                    "vehicle.skipped.idempotent",
                    idempotency_key=idem_key,
                    source_url=vehicle.get("source_url"),
                    make=vehicle.get("make"),
                    model=vehicle.get("model"),
                    variant=vehicle.get("variant"),
                )
                continue

            try:
                self._validate(vehicle)
                with self._persister.transaction():
                    self._persister.upsert(vehicle, context)
                self._idempotency_store.remember(idem_key)
                written += 1
            except ValueError as exc:
                failed += 1
                self._log_vehicle_error(
                    vehicle=vehicle,
                    validation_stage="validation",
                    reason=str(exc),
                    idempotency_key=idem_key,
                )
                self._send_to_dlq(
                    message=message,
                    vehicle=vehicle,
                    validation_stage="validation",
                    reason=str(exc),
                    idempotency_key=idem_key,
                    recoverable=False,
                )
            except Exception as exc:  # noqa: BLE001
                failed += 1
                self._log_vehicle_error(
                    vehicle=vehicle,
                    validation_stage="persistence",
                    reason=str(exc),
                    idempotency_key=idem_key,
                )
                self._send_to_dlq(
                    message=message,
                    vehicle=vehicle,
                    validation_stage="persistence",
                    reason=str(exc),
                    idempotency_key=idem_key,
                    recoverable=False,
                )

        return ProcessingSummary(written=written, failed=failed, skipped=skipped)

    def _send_to_dlq(
        self,
        *,
        message: Dict[str, Any],
        vehicle: Dict[str, Any],
        validation_stage: str,
        reason: str,
        idempotency_key: str,
        recoverable: bool,
    ) -> None:
        payload = {
            "message_context": {
                "market": message.get("market"),
                "source": message.get("source"),
                "fetched_at": message.get("fetched_at"),
                "request_id": message.get("request_id"),
            },
            "vehicle": vehicle,
            "diagnostics": {
                "validation_stage": validation_stage,
                "reason": reason,
                "idempotency_key": idempotency_key,
                "recoverable": recoverable,
            },
        }
        self._dlq_client.send(payload)

    def _log_vehicle_error(
        self,
        *,
        vehicle: Dict[str, Any],
        validation_stage: str,
        reason: str,
        idempotency_key: str,
    ) -> None:
        self._logger.error(
            "vehicle.process.failed",
            source_url=vehicle.get("source_url"),
            make=vehicle.get("make"),
            model=vehicle.get("model"),
            variant=vehicle.get("variant"),
            validation_stage=validation_stage,
            reason=reason,
            idempotency_key=idempotency_key,
        )

    def _idempotency_key(self, context: Dict[str, Any], vehicle: Dict[str, Any]) -> str:
        fetched_at = _parse_datetime(context.get("fetched_at"))
        window = int(fetched_at.timestamp() // (self._idempotency_window_minutes * 60))
        raw = {
            "market": context.get("market"),
            "source_url": vehicle.get("source_url"),
            "variant": vehicle.get("variant"),
            "window": window,
        }
        encoded = json.dumps(raw, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @staticmethod
    def _default_validate(vehicle: Dict[str, Any]) -> None:
        required = ["source_url", "make", "model", "variant"]
        missing = [name for name in required if not vehicle.get(name)]
        if missing:
            raise ValueError(f"missing required fields: {', '.join(missing)}")


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, str) and value:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value).astimezone(timezone.utc)
    return datetime.now(timezone.utc)
