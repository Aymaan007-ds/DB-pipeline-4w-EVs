-- Upsert canonical field and persist all candidate evidence entries.
-- Input evidence JSONB array element shape:
-- {
--   "value": "2.0L",
--   "source_url": "https://example.com",
--   "source_type": "oem",          -- oem | regulator | aggregator | marketplace | unknown
--   "source_priority": 10,
--   "extracted_at": "2026-01-01T00:00:00Z",
--   "agent_name": "primary-agent",
--   "confidence": 0.96,
--   "quote_snippet": "Engine: 2.0L"
-- }
CREATE OR REPLACE FUNCTION upsert_spec_with_evidence(
    p_vehicle_id TEXT,
    p_field_name TEXT,
    p_final_value TEXT,
    p_resolution_reason TEXT,
    p_evidence JSONB,
    p_high_priority_threshold INTEGER DEFAULT 20
)
RETURNS TABLE(
    vehicle_id TEXT,
    field_name TEXT,
    value TEXT,
    status TEXT,
    resolution_reason TEXT,
    conflict_group_id UUID
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_conflict BOOLEAN := FALSE;
    v_conflict_group_id UUID := NULL;
BEGIN
    IF p_evidence IS NULL OR jsonb_typeof(p_evidence) <> 'array' THEN
        RAISE EXCEPTION 'p_evidence must be a JSON array';
    END IF;

    -- Persist every candidate from primary/fallback agents.
    INSERT INTO spec_evidence (
        vehicle_id,
        field_name,
        value,
        source_url,
        source_type,
        source_priority,
        extracted_at,
        agent_name,
        confidence,
        quote_snippet,
        conflict_group_id
    )
    SELECT
        p_vehicle_id,
        p_field_name,
        item.value ->> 'value',
        item.value ->> 'source_url',
        COALESCE(item.value ->> 'source_type', 'unknown'),
        COALESCE((item.value ->> 'source_priority')::INTEGER, 100),
        COALESCE((item.value ->> 'extracted_at')::TIMESTAMPTZ, NOW()),
        COALESCE(item.value ->> 'agent_name', 'unknown-agent'),
        COALESCE((item.value ->> 'confidence')::NUMERIC, 0),
        item.value ->> 'quote_snippet',
        NULL
    FROM jsonb_array_elements(p_evidence) AS item;

    -- Conflict definition: multiple distinct values from high-priority sources.
    SELECT COUNT(DISTINCT se.value) > 1
    INTO v_conflict
    FROM spec_evidence se
    WHERE se.vehicle_id = p_vehicle_id
      AND se.field_name = p_field_name
      AND se.source_priority <= p_high_priority_threshold
      AND se.value IS NOT NULL;

    IF v_conflict THEN
        v_conflict_group_id := gen_random_uuid();

        UPDATE spec_evidence
        SET conflict_group_id = v_conflict_group_id
        WHERE vehicle_id = p_vehicle_id
          AND field_name = p_field_name
          AND source_priority <= p_high_priority_threshold
          AND value IS NOT NULL;
    END IF;

    -- Canonical upsert with status + resolution metadata.
    INSERT INTO specs (
        vehicle_id,
        field_name,
        value,
        status,
        resolution_reason,
        resolved_from_conflict_group,
        updated_at
    )
    VALUES (
        p_vehicle_id,
        p_field_name,
        p_final_value,
        CASE WHEN v_conflict THEN 'conflicted' ELSE 'resolved' END,
        p_resolution_reason,
        v_conflict_group_id,
        NOW()
    )
    ON CONFLICT (vehicle_id, field_name)
    DO UPDATE
      SET value = EXCLUDED.value,
          status = EXCLUDED.status,
          resolution_reason = EXCLUDED.resolution_reason,
          resolved_from_conflict_group = EXCLUDED.resolved_from_conflict_group,
          updated_at = NOW();

    RETURN QUERY
    SELECT
        s.vehicle_id,
        s.field_name,
        s.value,
        s.status,
        s.resolution_reason,
        s.resolved_from_conflict_group
    FROM specs s
    WHERE s.vehicle_id = p_vehicle_id
      AND s.field_name = p_field_name;
END;
$$;
