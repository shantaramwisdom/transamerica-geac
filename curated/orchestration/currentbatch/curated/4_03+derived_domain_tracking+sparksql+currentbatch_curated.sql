WITH
batches_ AS (
    select *,
           row_number() over (partition by source_system_id,cycle_date order by rnk
           ) as fltr
    from (
        select distinct cycle_date,
            CAST(date_format(cycle_date, 'yyyyMMdd') AS INT) as cycle_date_int,
            lpad({source_system_id}, 5, 0) source_system_id, rnk
        from curatedbatches
    )
)
SELECT DISTINCT '{source_system_name}' as source_system_name,
        '{domain_name}' as domain_name,
        '{batch_frequency}' as batch_frequency,
        cast(
            a.cycle_date_int || a.source_system_id || 
            case
                when b.batch_id is null then a.fltr
                else replace(b.batch_id,date_format(b.cycle_date, 'yyyyMMdd') || a.source_system_id,'') + a.fltr
            end as bigint
        ) AS batch_id,
        CAST(a.cycle_date AS DATE) AS cycle_date,
        c.source_system_name as base_source_system_name,
        c.domain_name as base_domain_name,
        cast(c.batch_id as bigint) as base_batch_id
FROM batches_ a
    LEFT JOIN latestcompletedbatch b using (cycle_date)
    cross join curatedbatches c using (cycle_date, rnk)
;