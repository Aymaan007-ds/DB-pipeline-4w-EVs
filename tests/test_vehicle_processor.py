from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, List

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.vehicle_processor import InMemoryIdempotencyStore, VehicleMessageProcessor


@dataclass
class LogEntry:
    event: str
    fields: Dict[str, Any]


class StubLogger:
    def __init__(self) -> None:
        self.infos: List[LogEntry] = []
        self.errors: List[LogEntry] = []

    def info(self, event: str, **fields: Any) -> None:
        self.infos.append(LogEntry(event, fields))

    def error(self, event: str, **fields: Any) -> None:
        self.errors.append(LogEntry(event, fields))


class StubDlq:
    def __init__(self) -> None:
        self.payloads: List[Dict[str, Any]] = []

    def send(self, payload: Dict[str, Any]) -> None:
        self.payloads.append(payload)


class StubPersister:
    def __init__(self) -> None:
        self.written: List[str] = []
        self.open_transactions = 0

    @contextmanager
    def transaction(self):
        self.open_transactions += 1
        try:
            yield
        finally:
            self.open_transactions -= 1

    def upsert(self, vehicle: Dict[str, Any], context: Dict[str, Any]) -> None:
        if vehicle.get("source_url") == "https://bad/write":
            raise RuntimeError("db write failed")
        self.written.append(vehicle["source_url"])



def _message() -> Dict[str, Any]:
    return {
        "market": "us",
        "source": "inventory",
        "fetched_at": "2025-01-01T10:04:00Z",
        "request_id": "req-1",
        "vehicles": [
            {
                "source_url": "https://ok/1",
                "make": "Toyota",
                "model": "RAV4",
                "variant": "XLE",
            },
            {
                "source_url": "https://bad/write",
                "make": "Honda",
                "model": "CR-V",
                "variant": "Sport",
            },
            {
                "source_url": "https://bad/validation",
                "make": "Ford",
                "model": "Escape",
                "variant": "",
            },
            {
                "source_url": "https://ok/2",
                "make": "Tesla",
                "model": "Model Y",
                "variant": "Long Range",
            },
        ],
    }


def test_per_vehicle_transactions_continue_after_failures() -> None:
    persister = StubPersister()
    logger = StubLogger()
    dlq = StubDlq()
    idem = InMemoryIdempotencyStore()

    processor = VehicleMessageProcessor(
        persister=persister,
        idempotency_store=idem,
        dlq_client=dlq,
        logger=logger,
    )

    summary = processor.process_message(_message())

    assert summary.written == 2
    assert summary.failed == 2
    assert summary.skipped == 0
    assert persister.written == ["https://ok/1", "https://ok/2"]
    assert persister.open_transactions == 0

    assert len(logger.errors) == 2
    first_error = logger.errors[0]
    assert first_error.fields["source_url"] == "https://bad/write"
    assert first_error.fields["validation_stage"] == "persistence"

    second_error = logger.errors[1]
    assert second_error.fields["source_url"] == "https://bad/validation"
    assert second_error.fields["validation_stage"] == "validation"

    assert len(dlq.payloads) == 2
    assert dlq.payloads[0]["diagnostics"]["reason"] == "db write failed"
    assert dlq.payloads[1]["diagnostics"]["validation_stage"] == "validation"


def test_idempotency_skips_duplicate_redelivery() -> None:
    persister = StubPersister()
    logger = StubLogger()
    dlq = StubDlq()
    idem = InMemoryIdempotencyStore()

    processor = VehicleMessageProcessor(
        persister=persister,
        idempotency_store=idem,
        dlq_client=dlq,
        logger=logger,
    )

    message = {
        "market": "us",
        "source": "inventory",
        "fetched_at": "2025-01-01T10:04:00Z",
        "request_id": "req-2",
        "vehicles": [
            {
                "source_url": "https://dup/1",
                "make": "BMW",
                "model": "X3",
                "variant": "xDrive",
            }
        ],
    }

    first = processor.process_message(message)
    second = processor.process_message(message)

    assert (first.written, first.failed, first.skipped) == (1, 0, 0)
    assert (second.written, second.failed, second.skipped) == (0, 0, 1)
    assert len(logger.infos) == 1
    assert logger.infos[0].event == "vehicle.skipped.idempotent"
    assert len(persister.written) == 1
