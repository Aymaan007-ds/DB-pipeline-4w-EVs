# Coordinator Workflow

This repository now includes a resilient, guard-railed coordinator workflow for vehicle-spec extraction.

## Implemented stages

1. `PrimaryAgent`: current extraction entrypoint from `source_url`.
2. `GapDetector`: compares against required-field policy and emits missing targets.
3. `SearchAgent`: constrained search for missing targets using domain allowlist.
4. `VerifierAgent`: validates against source-priority and multi-source rules.
5. `MergerAgent`: merges enrichment with provenance/conflict flags/confidence.
6. `CommitAgent`: commits only verified fields; unresolved values remain null with explicit reasons.

## Guard rails

- Max hops: `MAX_HOPS = 2`
- Max retries per stage: `STAGE_BUDGETS[*].max_retries`
- Domain allowlist: `TRUSTED_DOMAIN_ALLOWLIST`
- Source priority: `SOURCE_PRIORITY`
- Strict JSON schema checks at each stage boundary
- Token/time budgets per stage with fail-fast timeout behavior
- Fail-closed conflict handling (`contradictory_sources`, `conflict_detected_fail_closed`)

## Files

- `lambda_function.py`: Lambda orchestrator and per-stage handlers.
- `step_functions/coordinator.asl.json`: Step Functions state machine definition (preferred resilient runtime).

## Run locally

```bash
python - <<'PY'
from lambda_function import lambda_handler
print(lambda_handler({"source_url": "https://example.com/car"}, None))
PY
```
