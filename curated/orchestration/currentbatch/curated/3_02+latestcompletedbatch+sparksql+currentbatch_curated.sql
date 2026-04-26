SELECT
    nvl(max(batch_id), 0) batch_id,
    max(cycle_date) cycle_date,
    domain_name,
    source_system,
    batch_frequency
FROM
    {curated_database}.completedbatch
WHERE
    source_system = '{source_system_name}'
    AND domain_name = '{domain_name}'
    AND batch_frequency = '{batch_frequency}'
GROUP BY
    domain_name,
    source_system,
    batch_frequency;