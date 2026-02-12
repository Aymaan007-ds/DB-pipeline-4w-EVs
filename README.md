# Multi-agent EV ingestion Lambda (SQS -> Postgres)

This repo now ships a **deployable Lambda** that keeps the multi-agent guardrail approach and persists curated data into your PostgreSQL DDL (`vehicles`, `specs`).

## What the Lambda does

For each SQS message:
1. **PrimaryAgent**: reads `raw_extraction` from message.
2. **GapDetector**: finds missing required fields.
3. **SearchAgent**: reads `enrichment_candidates` and filters by trusted domain allowlist.
4. **VerifierAgent**: fail-closed verification (contradictions and weak evidence are unresolved).
5. **MergerAgent**: merges verified values, nulls on conflicts.
6. **CommitAgent**: emits final `record` + explicit `null_reasons`.
7. Persists to Postgres with upserts into `vehicles` and `specs`.

## Required env vars

Either set:
- `DB_DSN`

Or set:
- `DB_HOST`
- `DB_PORT` (optional, defaults to `5432`)
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `DB_SSLMODE` (optional, defaults to `require`)

Optional:
- `TRUSTED_DOMAIN_ALLOWLIST` (comma-separated list)

## SQS message format

```json
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
    "notes": "variant dependent charging",
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
    }
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
```

## Deploy on AWS Lambda

### 1) Package code + dependency layer

Use either `psycopg` or `psycopg2` in your deployment artifact.

Example (zip deployment, psycopg2):

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install psycopg2-binary -t package/
cp lambda_function.py package/
(cd package && zip -r ../lambda_bundle.zip .)
```

If you need `src/` utilities in the future, copy `src/` into `package/` as well.

### 2) Create/update Lambda

- Runtime: Python 3.11+
- Handler: `lambda_function.lambda_handler`
- Upload `lambda_bundle.zip`
- Set env vars listed above
- Ensure Lambda network can reach your Postgres (VPC/subnets/security groups as needed)

### 3) Configure SQS trigger

- Add SQS event source mapping
- Enable **Report batch item failures**
- Choose batch size according to DB throughput (start with 5–10)
- Configure SQS DLQ redrive policy for poison messages

### 4) Smoke test

Use direct invoke payload (same shape as a message body). For SQS path, send the body JSON to queue and verify CloudWatch logs.

## Local sanity check

```bash
python -m py_compile lambda_function.py
```
