# DB pipeline persistence model

This repository now includes SQL migrations for canonical specs + evidence persistence:

- `sql/000_extensions.sql`: enables `pgcrypto` for `gen_random_uuid()`.
- `sql/001_init_specs_and_evidence.sql`: creates `specs` and `spec_evidence` tables.
- `sql/002_upsert_specs_with_evidence.sql`: adds `upsert_spec_with_evidence(...)` for canonical upsert + evidence capture + conflict detection.

## Conflict behavior

`upsert_spec_with_evidence` marks a field as `status='conflicted'` when two or more distinct candidate values appear from high-priority sources (`source_priority <= p_high_priority_threshold`, default `20`).

The function also:

1. Upserts the final canonical value into `specs`.
2. Persists all candidate rows into `spec_evidence`.
3. Assigns a `conflict_group_id` to high-priority conflicting evidence rows.
4. Stores `resolution_reason` on the canonical row.
