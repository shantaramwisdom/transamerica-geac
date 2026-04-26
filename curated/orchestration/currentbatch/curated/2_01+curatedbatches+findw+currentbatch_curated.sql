WITH datawarehouse AS (
    select *
    from (
        select source_system_name,domain_name,batch_id,cycle_date,phase_name,load_status,
               row_number() over(partition by source_system_name,domain_name,phase_name
                                 order by batch_id desc, insert_timestamp desc) fltr
        from financeadorchestration.batch_tracking bt
        where phase_name = 'datawarehouse'
          and domain_name = '{domain_name}'
          and load_status = 'complete'
          and source_system_name = '{source_system_name}'
    )
    where fltr = 1
),
curated_completed as (
    select *
    from (
        SELECT source_system_name,domain_name,batch_id,cycle_date,base_source_system_name,
               base_domain_name,base_batch_id,
               row_number() over (partition by base_source_system_name,base_domain_name
                                  order by cycle_date desc,base_batch_id desc,insert_timestamp desc
               ) as fltr
        FROM financeadorchestration.derived_domain_tracking
        where curated_load_complete_flag = 'Y'
          and domain_name = '{domain_name}'
          and source_system_name = '{source_system_name}'
          and batch_frequency = '{batch_frequency}'
    )
    where fltr = 1
)
select distinct a.source_system_name,
                a.domain_name,
                a.batch_id,
                a.cycle_date,
                a.phase_name,
                a.load_status,
                /*dense_rank() over(
                    partition by a.source_system_name,
                    a.cycle_date
                    order by a.batch_id
                ) rnk*/
                {dense_rank}
from financeadworchestration.batch_tracking a
    left join datawarehouse b on a.phase_name = b.phase_name
    left join curated_completed c on a.source_system_name = c.base_source_system_name
    and a.domain_name = c.base_domain_name
where a.phase_name = 'datawarehouse'
  and a.load_status = 'complete'
  and a.source_system_name in ({drvd_domn_source_ss})
  and a.domain_name in ({drvd_domn_source_dm})
  and {curated_initial_cut_off}
  and (
        a.cycle_date > nvl(c.cycle_date, '1900-01-01')
       or a.batch_id > nvl(c.base_batch_id, 0)
  )
order by 4, 3, 1, 2;