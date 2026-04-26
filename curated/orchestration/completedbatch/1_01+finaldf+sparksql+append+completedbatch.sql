SELECT DISTINCT a.batch_id,
                a.cycle_date,
                from_utc_timestamp(CURRENT_TIMESTAMP, 'US/Central') recorded_timestamp,
                a.domain_name,
                a.source_system,
                a.batch_frequency
FROM {curated_database}.currentbatch a
LEFT JOIN {curated_database}.completedbatch b USING (domain_name,
                                                     source_system,
                                                     batch_frequency,
                                                     batch_id)
WHERE a.domain_name = '{domain_name}'
  AND a.source_system = '{source_system_name}'
  AND a.batch_frequency = '{batch_frequency}'
  AND CAST(a.batch_id AS INT) != NVL(CAST(b.batch_id AS INT), 0)
;