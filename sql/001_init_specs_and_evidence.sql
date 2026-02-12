-- Canonical specs table (one row per extracted field).
CREATE TABLE IF NOT EXISTS specs (
    vehicle_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    value TEXT,
    status TEXT NOT NULL DEFAULT 'resolved' CHECK (status IN ('resolved', 'conflicted', 'unresolved')),
    resolution_reason TEXT,
    resolved_from_conflict_group UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (vehicle_id, field_name)
);

-- Evidence table keeps every candidate value produced by extraction agents.
CREATE TABLE IF NOT EXISTS spec_evidence (
    id BIGSERIAL PRIMARY KEY,
    vehicle_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    value TEXT,
    source_url TEXT,
    source_type TEXT NOT NULL,
    source_priority INTEGER NOT NULL,
    extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    agent_name TEXT NOT NULL,
    confidence NUMERIC(5,4) CHECK (confidence >= 0 AND confidence <= 1),
    quote_snippet TEXT,
    conflict_group_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_spec_evidence_lookup
    ON spec_evidence (vehicle_id, field_name, extracted_at DESC);

CREATE INDEX IF NOT EXISTS idx_spec_evidence_conflict
    ON spec_evidence (conflict_group_id)
    WHERE conflict_group_id IS NOT NULL;
