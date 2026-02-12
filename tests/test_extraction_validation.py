import unittest

from extraction_validation import (
    InMemoryDeadLetterQueue,
    ValidationException,
    validate_and_canonicalize_extraction,
)


class ExtractionValidationTests(unittest.TestCase):
    def test_valid_payload_coerces_and_normalizes(self) -> None:
        queue = InMemoryDeadLetterQueue()
        payload = {
            "request_id": "req-1",
            "generated_at": "2026-01-01T00:00:00Z",
            "producer": "gpt",
            "vehicles": [
                {
                    "source_id": "v1",
                    "make": "vw",
                    "model": "ioniq5",
                    "variant": "long range awd",
                    "model_year": 2024,
                    "specs": {
                        "horsepower_kw": "150 kW",
                        "torque_nm": "350 Nm",
                        "battery_kwh": "77.4 kWh",
                        "range_km": "480 km",
                        "zero_to_hundred_s": "5.2 sec",
                        "top_speed_kmh": "185 km/h",
                        "curb_weight_kg": "2100 kg",
                        "price_usd": "$52,990",
                    },
                }
            ],
        }

        envelope = validate_and_canonicalize_extraction(payload, queue)

        vehicle = envelope.vehicles[0]
        self.assertEqual(vehicle.make, "Volkswagen")
        self.assertEqual(vehicle.model, "Ioniq 5")
        self.assertEqual(vehicle.variant, "Long Range All-Wheel Drive")
        self.assertEqual(vehicle.specs.price_usd, 52990)
        self.assertEqual(queue.messages, [])

    def test_invalid_rows_are_quarantined(self) -> None:
        queue = InMemoryDeadLetterQueue()
        payload = {
            "request_id": "req-2",
            "generated_at": "2026-01-01T00:00:00Z",
            "producer": "gpt",
            "vehicles": [
                {
                    "source_id": "ok",
                    "make": "BMW AG",
                    "model": "i4",
                    "variant": "m50 awd",
                    "model_year": 2023,
                    "specs": {"range_km": "450 km"},
                },
                {
                    "source_id": "bad",
                    "make": "Chevy",
                    "model": "bolt",
                    "variant": "lt",
                    "model_year": 2024,
                    "specs": {"range_km": "10000 km"},
                },
            ],
        }

        envelope = validate_and_canonicalize_extraction(payload, queue)

        self.assertEqual(len(envelope.vehicles), 1)
        self.assertEqual(envelope.vehicles[0].source_id, "ok")
        self.assertEqual(len(queue.messages), 1)
        self.assertEqual(queue.messages[0]["scope"], "vehicle")

    def test_rejects_invalid_envelope(self) -> None:
        queue = InMemoryDeadLetterQueue()

        with self.assertRaises(ValidationException):
            validate_and_canonicalize_extraction({"producer": "gpt", "vehicles": []}, queue)

        self.assertEqual(len(queue.messages), 1)
        self.assertEqual(queue.messages[0]["scope"], "envelope")


if __name__ == "__main__":
    unittest.main()
