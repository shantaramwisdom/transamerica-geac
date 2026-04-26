SELECT DISTINCT batch_id,
    cycle_date,
    from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') AS recorded_timestamp,
    domain_name,
    source_system_name as source_system,
    batch_frequency
FROM derived_domain_tracking;